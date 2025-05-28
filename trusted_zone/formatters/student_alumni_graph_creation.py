import json
import os
from typing import Dict, List, Set, Tuple
from neo4j import GraphDatabase
import re
from collections import defaultdict, Counter
from itertools import combinations
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, collect_set, size, array_intersect
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType

class OptimizedNeo4jPropertyGraphBuilder:
    def __init__(self, uri: str, username: str, password: str, batch_size: int = 1000, max_workers: int = 4):
        """Initialize Neo4j connection with optimization parameters"""
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("Neo4jGraphBuilder") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
    def close(self):
        """Close connections"""
        self.driver.close()
        self.spark.stop()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared")
    
    def create_constraints_and_indexes(self):
        """Create uniqueness constraints and indexes"""
        constraints = [
            "CREATE CONSTRAINT student_name_unique IF NOT EXISTS FOR (s:Student) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT alumni_name_unique IF NOT EXISTS FOR (a:Alumni) REQUIRE a.name IS UNIQUE", 
            "CREATE CONSTRAINT country_name_unique IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT degree_name_unique IF NOT EXISTS FOR (d:Degree) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT skill_name_unique IF NOT EXISTS FOR (sk:Skill) REQUIRE sk.name IS UNIQUE",
            "CREATE CONSTRAINT company_name_unique IF NOT EXISTS FOR (co:Company) REQUIRE co.name IS UNIQUE",
            "CREATE CONSTRAINT award_name_unique IF NOT EXISTS FOR (aw:Award) REQUIRE aw.name IS UNIQUE",
            "CREATE CONSTRAINT scholarship_name_unique IF NOT EXISTS FOR (sc:Scholarship) REQUIRE sc.name IS UNIQUE",
            "CREATE CONSTRAINT institution_name_unique IF NOT EXISTS FOR (i:Institution) REQUIRE i.name IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Constraint creation warning: {e}")
        
        print("Constraints and indexes created")
    
    def load_data(self, student_file: str, alumni_file: str):
        """Load data using Spark for faster processing"""
        # Load student data with Spark
        students_df = None
        if os.path.exists(student_file):
            # Read JSONL file
            students_rdd = self.spark.sparkContext.textFile(student_file)
            students_rdd = students_rdd.filter(lambda line: line.strip())
            students_json_rdd = students_rdd.map(lambda line: json.loads(line))
            students_df = self.spark.createDataFrame(students_json_rdd)
        
        # Load alumni data
        alumni_data = {}
        if os.path.exists(alumni_file):
            with open(alumni_file, 'r', encoding='utf-8') as f:
                alumni_data = json.load(f)
        
        return students_df, alumni_data
    
    def extract_entities_with_spark(self, students_df, alumni_data: Dict):
        """Extract unique entities using Spark for parallel processing"""
        entities = {
            'countries': set(),
            'degrees': set(),
            'skills': set(),
            'companies': set(),
            'awards': [],
            'scholarships': set(),
            'institutions': set()
        }
        
        if students_df:
            # Extract from students using Spark
            countries = students_df.select("country").distinct().filter(col("country").isNotNull()).collect()
            entities['countries'].update([row.country for row in countries])
            
            degrees = students_df.select("major").distinct().filter(col("major").isNotNull()).collect()
            entities['degrees'].update([row.major for row in degrees])
            
            # Extract hobbies/skills
            hobbies_df = students_df.select(explode("hobbies").alias("hobby")).filter(col("hobby").isNotNull())
            hobbies = hobbies_df.distinct().collect()
            entities['skills'].update([row.hobby for row in hobbies])
        
        # Process alumni data (keep original logic but optimize where possible)
        alumni_profiles = []
        for org_key, org_data in alumni_data.items():
            if org_data.get('title'):
                entities['scholarships'].update(org_data['title'])
            
            for profile in org_data.get('profiles', []):
                alumni_profiles.append(profile)
                
                if profile.get('skills'):
                    for skill_item in profile['skills']:
                        if isinstance(skill_item, dict) and skill_item.get('skill'):
                            entities['skills'].add(skill_item['skill'])
                
                if profile.get('experience'):
                    for exp in profile['experience']:
                        if exp.get('company'):
                            company = re.sub(r'\s*·.*$', '', exp['company']).strip()
                            if company:
                                entities['companies'].add(company)
                
                if profile.get('education'):
                    for edu in profile['education']:
                        if edu.get('institution'):
                            entities['institutions'].add(edu['institution'])
                        if edu.get('degree'):
                            degree = edu['degree'].split(',')[0].strip()
                            entities['degrees'].add(degree)
                
                if profile.get('honors_awards'):
                    for award in profile['honors_awards']:
                        if isinstance(award, dict) and award.get('award'):
                            description = award.get('description', '') or ''
                            entities['awards'].append({
                                'name': award['award'],
                                'description': description,
                                'date': award.get('date', '') or '',
                                'associated_with': award.get('associated_with', '') or ''
                            })
        
        return entities, alumni_profiles
    
    def batch_execute(self, query: str, parameters_list: List[Dict], operation_name: str):
        """Execute queries in batches for better performance"""
        total_batches = len(parameters_list) // self.batch_size + (1 if len(parameters_list) % self.batch_size else 0)
        
        print(f"Executing {operation_name} in {total_batches} batches...")
        
        def execute_batch(batch_params):
            with self.driver.session() as session:
                for params in batch_params:
                    try:
                        session.run(query, **params)
                    except Exception as e:
                        print(f"Error in {operation_name}: {e}")
                        continue
        
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for i in range(0, len(parameters_list), self.batch_size):
                batch = parameters_list[i:i + self.batch_size]
                futures.append(executor.submit(execute_batch, batch))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Batch execution error: {e}")
    
    def create_nodes_optimized(self, entities: Dict, students_df, alumni_profiles: List[Dict]):
        """Create nodes using batch operations"""
        start_time = time.time()
        
        # Create entity nodes in batches
        node_operations = [
            ("Country", "MERGE (c:Country {name: $name})", [{'name': name} for name in entities['countries']]),
            ("Degree", "MERGE (d:Degree {name: $name})", [{'name': name} for name in entities['degrees']]),
            ("Company", "MERGE (c:Company {name: $name})", [{'name': name} for name in entities['companies']]),
            ("Institution", "MERGE (i:Institution {name: $name})", [{'name': name} for name in entities['institutions']]),
            ("Scholarship", "MERGE (s:Scholarship {name: $name})", [{'name': name} for name in entities['scholarships']])
        ]
        
        for node_type, query, params in node_operations:
            self.batch_execute(query, params, f"Creating {node_type} nodes")
        
        # Create Skill nodes with type classification
        skill_params = []
        for skill in entities['skills']:
            skill_params.append({'name': skill})

        self.batch_execute(
            "MERGE (s:Skill {name: $name})",  # Removed SET s.skill_type = $skill_type
            skill_params,
            "Creating Skill nodes"
        )
        
        # Create Award nodes
        processed_awards = set()
        award_params = []
        for award in entities['awards']:
            if award['name'] not in processed_awards:
                award_params.append({
                    'name': award['name'],
                    'description': award['description']
                })
                processed_awards.add(award['name'])
        
        self.batch_execute(
            "MERGE (a:Award {name: $name}) SET a.description = $description",
            award_params,
            "Creating Award nodes"
        )
        
        # Create Student nodes
        if students_df:
            students_data = students_df.collect()
            student_params = []
            for student in students_data:
                student_dict = student.asDict()
                student_params.append({
                    'name': student_dict.get('name', '') or '',
                    'age': student_dict.get('age'),
                    'sex': student_dict.get('sex', '') or '',
                    'story': student_dict.get('story', '') or '',
                    'unique_quality': student_dict.get('unique_quality', '') or '',
                    'state_province': student_dict.get('state_province', '') or ''
                })
            
            self.batch_execute(
                """
                MERGE (s:Student {name: $name})
                SET s.age = $age, s.sex = $sex, s.story = $story,
                    s.unique_quality = $unique_quality, s.state_province = $state_province
                """,
                student_params,
                "Creating Student nodes"
            )
        
        # Create Alumni nodes
        alumni_params = []
        for profile in alumni_profiles:
            alumni_params.append({
                'name': profile.get('name', '') or '',
                'url': profile.get('url', '') or '',
                'headline': profile.get('headline', '') or '',
                'about': profile.get('about', '') or ''
            })
        
        self.batch_execute(
            """
            MERGE (a:Alumni {name: $name})
            SET a.url = $url, a.headline = $headline, a.about = $about
            """,
            alumni_params,
            "Creating Alumni nodes"
        )
        
        print(f"Node creation completed in {time.time() - start_time:.2f} seconds")
    
    def create_relationships_optimized(self, students_df, alumni_data: Dict):
        """Create relationships using batch operations"""
        start_time = time.time()
        
        # Extract all institutions from alumni data for random assignment
        institutions = set()
        for org_data in alumni_data.values():
            for profile in org_data.get('profiles', []):
                if profile.get('education'):
                    for edu in profile['education']:
                        if edu.get('institution'):
                            institutions.add(edu['institution'])
        
        institutions_list = list(institutions)
        
        # Student relationships
        if students_df:
            students_data = students_df.collect()
            
            # Batch student-country relationships
            country_rels = []
            institution_rels = []  # Changed from degree_rels to institution_rels
            skill_rels = []
            
            for student in students_data:
                student_dict = student.asDict()
                student_name = student_dict.get('name', '') or ''
                
                if student_dict.get('country'):
                    country_rels.append({
                        'student_name': student_name,
                        'country': student_dict['country']
                    })
                
                # Randomly assign student to an institution with their major as degree
                if student_dict.get('major') and institutions_list:
                    random_institution = random.choice(institutions_list)
                    random_year = random.randint(2010, 2025)
                    
                    institution_rels.append({
                        'student_name': student_name,
                        'institution': random_institution,
                        'degree': student_dict['major'],  # Use major as degree
                        'gpa': student_dict.get('gpa'),
                        'year': random_year
                    })
                
                if student_dict.get('hobbies'):
                    for hobby in student_dict['hobbies']:
                        if hobby:
                            skill_rels.append({
                                'student_name': student_name,
                                'hobby': hobby
                            })
            
            # Execute relationship batches
            self.batch_execute(
                """
                MATCH (s:Student {name: $student_name})
                MATCH (c:Country {name: $country})
                MERGE (s)-[:LIVES_IN]->(c)
                """,
                country_rels,
                "Creating Student-Country relationships"
            )
            
            # Changed to create Student -> STUDIES -> Institution relationship
            self.batch_execute(
                """
                MATCH (s:Student {name: $student_name})
                MATCH (i:Institution {name: $institution})
                MERGE (s)-[r:STUDIES]->(i)
                SET r.degree = $degree, r.gpa = $gpa, r.year = $year
                """,
                institution_rels,
                "Creating Student-Institution relationships"
            )
            
            self.batch_execute(
                """
                MATCH (s:Student {name: $student_name})
                MATCH (sk:Skill {name: $hobby})
                MERGE (s)-[:HAS_SKILL]->(sk)
                """,
                skill_rels,
                "Creating Student-Skill relationships"
            )
        
        # Alumni relationships (similar batching approach)
        self._create_alumni_relationships_batched(alumni_data)
        
        print(f"Relationship creation completed in {time.time() - start_time:.2f} seconds")
    
    def randomly_assign_skills_to_students(self, students_df, alumni_data: Dict, num_students: int = 100):
        """Randomly assign skills from alumni profiles to 100 students"""
        if not students_df:
            return
        
        # Collect all alumni skills
        alumni_skills = set()
        for org_data in alumni_data.values():
            for profile in org_data.get('profiles', []):
                if profile.get('skills'):
                    for skill_item in profile['skills']:
                        if isinstance(skill_item, dict) and skill_item.get('skill'):
                            alumni_skills.add(skill_item['skill'])
        
        alumni_skills_list = list(alumni_skills)
        if not alumni_skills_list:
            print("No alumni skills found")
            return
        
        # Get random sample of students
        students_data = students_df.collect()
        if len(students_data) < num_students:
            num_students = len(students_data)
        
        random_students = random.sample(students_data, num_students)
        
        # Create skill assignments
        skill_assignments = []
        for student in random_students:
            student_name = student.asDict().get('name', '') or ''
            # Assign 2-5 random skills to each student
            num_skills = random.randint(2, 5)
            assigned_skills = random.sample(alumni_skills_list, min(num_skills, len(alumni_skills_list)))
            
            for skill in assigned_skills:
                skill_assignments.append({
                    'student_name': student_name,
                    'skill': skill
                })
        
        # Execute skill assignments
        self.batch_execute(
            """
            MATCH (s:Student {name: $student_name})
            MATCH (sk:Skill {name: $skill})
            MERGE (s)-[:HAS_SKILL]->(sk)
            """,
            skill_assignments,
            "Creating random Student-Skill relationships"
        )
        
        print(f"Randomly assigned skills from alumni to {num_students} students")

    def _create_alumni_relationships_batched(self, alumni_data: Dict):
        """Create alumni relationships in batches"""
        scholarship_rels = []
        skill_rels = []
        company_rels = []
        institution_rels = []
        award_rels = []
        
        for org_key, org_data in alumni_data.items():
            scholarship_names = org_data.get('title', [])
            
            for profile in org_data.get('profiles', []):
                alumni_name = profile.get('name', '') or ''
                
                # Scholarship relationships
                for scholarship in scholarship_names:
                    scholarship_rels.append({
                        'alumni_name': alumni_name,
                        'scholarship': scholarship
                    })
                
                # Skill relationships
                if profile.get('skills'):
                    for skill_item in profile['skills']:
                        if isinstance(skill_item, dict) and skill_item.get('skill'):
                            skill_rels.append({
                                'alumni_name': alumni_name,
                                'skill_name': skill_item['skill'],
                                'endorsements': skill_item.get('endorsements', '') or ''
                            })
                
                # Company relationships
                if profile.get('experience'):
                    for exp in profile['experience']:
                        company = re.sub(r'\s*·.*$', '', exp.get('company', '')).strip()
                        if company:
                            company_rels.append({
                                'alumni_name': alumni_name,
                                'company': company,
                                'title': exp.get('title', '') or '',
                                'duration': exp.get('duration', '') or ''
                            })
                
                # Institution relationships
                if profile.get('education'):
                    for edu in profile['education']:
                        if edu.get('institution'):
                            degree = edu.get('degree', '').split(',')[0].strip() if edu.get('degree') else ''
                            institution_rels.append({
                                'alumni_name': alumni_name,
                                'institution': edu['institution'],
                                'degree': degree,
                                'dates': edu.get('dates', '') or ''
                            })
                
                # Award relationships
                if profile.get('honors_awards'):
                    for award in profile['honors_awards']:
                        if isinstance(award, dict) and award.get('award'):
                            award_rels.append({
                                'alumni_name': alumni_name,
                                'award_name': award['award'],
                                'date': award.get('date', '') or '',
                                'associated_with': award.get('associated_with', '') or ''
                            })
        
        # Execute all alumni relationship batches
        relationship_batches = [
            ("Alumni-Scholarship", """
                MATCH (a:Alumni {name: $alumni_name})
                MATCH (s:Scholarship {name: $scholarship})
                MERGE (a)-[:RECIPIENT_OF]->(s)
            """, scholarship_rels),
            ("Alumni-Skill", """
                MATCH (a:Alumni {name: $alumni_name})
                MATCH (s:Skill {name: $skill_name})
                MERGE (a)-[r:HAS_SKILL]->(s)
                SET r.endorsements = $endorsements
            """, skill_rels),
            ("Alumni-Company", """
                MATCH (a:Alumni {name: $alumni_name})
                MATCH (c:Company {name: $company})
                MERGE (a)-[r:WORKED_AT]->(c)
                SET r.title = $title, r.duration = $duration
            """, company_rels),
            ("Alumni-Institution", """
                MATCH (a:Alumni {name: $alumni_name})
                MATCH (i:Institution {name: $institution})
                MERGE (a)-[r:STUDIED_AT]->(i)
                SET r.degree = $degree, r.dates = $dates
            """, institution_rels),
            ("Alumni-Award", """
                MATCH (a:Alumni {name: $alumni_name})
                MATCH (aw:Award {name: $award_name})
                MERGE (a)-[r:HAS_AWARD]->(aw)
                SET r.date = $date, r.associated_with = $associated_with
            """, award_rels)
        ]
        
        for rel_type, query, params in relationship_batches:
            self.batch_execute(query, params, f"Creating {rel_type} relationships")
    
    def create_similarity_relationships_spark_optimized(self, students_df, alumni_data: Dict):
        """Create similarity relationships using optimized algorithms"""
        start_time = time.time()
        print("Creating similarity relationships with optimized approach...")
        
        # Prepare student data
        if students_df:
            # Get institution data for students (query Neo4j to get their assigned institutions)
            with self.driver.session() as session:
                student_institutions = session.run("""
                    MATCH (s:Student)-[r:STUDIES]->(i:Institution)
                    RETURN s.name as student_name, i.name as institution
                """).data()
                
                student_institution_map = {si['student_name']: si['institution'] for si in student_institutions}
            
            students_spark = students_df.select(
                col("name").alias("student_name"),
                col("hobbies").alias("skills"),
                col("major").alias("degree"),
                col("country")
            ).filter(col("student_name").isNotNull())
            
            students_data = students_spark.collect()
            student_list = []
            for row in students_data:
                skills = set(row.skills) if row.skills else set()
                skills = {s for s in skills if s}
                student_list.append({
                    'name': row.student_name,
                    'skills': skills,
                    'degree': row.degree or '',
                    'country': row.country or '',
                    'institution': student_institution_map.get(row.student_name, '')  # NEW
                })
        
        # Prepare alumni data
        alumni_list = []
        for org_data in alumni_data.values():
            for profile in org_data.get('profiles', []):
                skills = set()
                if profile.get('skills'):
                    skills = {s.get('skill', '') for s in profile['skills'] 
                            if isinstance(s, dict) and s.get('skill')}
                
                degree = ''
                institution = ''  # NEW
                if profile.get('education'):
                    for edu in profile['education']:
                        if edu.get('degree') and not degree:
                            degree = edu['degree'].split(',')[0].strip()
                        if edu.get('institution') and not institution:  # NEW
                            institution = edu['institution']
                
                alumni_list.append({
                    'name': profile.get('name', '') or '',
                    'skills': skills,
                    'degree': degree,
                    'institution': institution  # NEW
                })
        
        print(f"Processing {len(student_list)} students and {len(alumni_list)} alumni")
        
        # Use optimized similarity calculation
        self._calculate_similarities_optimized(student_list, alumni_list)
        
        print(f"Similarity relationships created in {time.time() - start_time:.2f} seconds")
    
    def _calculate_similarities_optimized(self, student_list: List[Dict], alumni_list: List[Dict]):
        """Optimized similarity calculation using indexing and sampling (FIXED VERSION)"""
        
        # 1. CREATE SKILL-BASED INDEXES
        skill_to_students = defaultdict(list)
        skill_to_alumni = defaultdict(list)
        degree_to_students = defaultdict(list)
        degree_to_alumni = defaultdict(list)
        institution_to_students = defaultdict(list)
        institution_to_alumni = defaultdict(list)
        
        # Index students by skills, degrees, and institutions
        for i, student in enumerate(student_list):
            for skill in student['skills']:
                if skill.strip():  # Make sure skill is not empty
                    skill_to_students[skill.lower()].append(i)  # Use lowercase for matching
            if student['degree']:
                degree_to_students[student['degree'].lower()].append(i)
            if student.get('institution'):
                institution_to_students[student['institution'].lower()].append(i)
        
        # Index alumni by skills, degrees, and institutions
        for i, alumni in enumerate(alumni_list):
            for skill in alumni['skills']:
                if skill.strip():  # Make sure skill is not empty
                    skill_to_alumni[skill.lower()].append(i)  # Use lowercase for matching
            if alumni['degree']:
                degree_to_alumni[alumni['degree'].lower()].append(i)
            if alumni.get('institution'):
                institution_to_alumni[alumni['institution'].lower()].append(i)
        
        print("Created skill, degree, and institution indexes")
        print(f"Alumni skills indexed: {len(skill_to_alumni)} unique skills")
        print(f"Alumni degrees indexed: {len(degree_to_alumni)} unique degrees")
        print(f"Student skills indexed: {len(skill_to_students)} unique skills")
        
        # Debug: Print some sample skills to check data
        if skill_to_alumni:
            print(f"Sample alumni skills: {list(skill_to_alumni.keys())[:10]}")
        if skill_to_students:
            print(f"Sample student skills: {list(skill_to_students.keys())[:10]}")
        
        # 2. STUDENT-TO-STUDENT SIMILARITIES (keep existing logic)
        print("Calculating student-to-student similarities...")
        student_similarities = []
        processed_pairs = set()
        
        def process_student_batch(student_indices):
            batch_similarities = []
            for i in student_indices:
                student = student_list[i]
                candidates = set()
                
                # Find candidates through shared skills (most selective)
                for skill in student['skills']:
                    if skill.strip():
                        candidates.update(skill_to_students[skill.lower()])
                
                # Add candidates through shared degrees
                if student['degree']:
                    candidates.update(degree_to_students[student['degree'].lower()])
                
                # Remove self and limit candidates
                candidates.discard(i)
                candidates = list(candidates)[:100]
                
                # Calculate similarities
                similarities = []
                for j in candidates:
                    if i != j:
                        pair = tuple(sorted([i, j]))
                        if pair not in processed_pairs:
                            processed_pairs.add(pair)
                            other_student = student_list[j]
                            score = self.calculate_similarity_score(
                                student['skills'], student['degree'], student['country'], student.get('institution', ''),
                                other_student['skills'], other_student['degree'], other_student['country'], other_student.get('institution', '')
                            )
                            if score > 0.1:
                                similarities.append((j, score))
                
                # Keep top 3 similarities
                similarities.sort(key=lambda x: x[1], reverse=True)
                for similar_idx, score in similarities[:3]:
                    batch_similarities.append({
                        'student1': student['name'],
                        'student2': student_list[similar_idx]['name'],
                        'score': score
                    })
            
            return batch_similarities
        
        # Process in parallel batches
        batch_size = max(100, len(student_list) // (self.max_workers * 4))
        student_batches = [list(range(i, min(i + batch_size, len(student_list)))) 
                        for i in range(0, len(student_list), batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_student_batch, batch) for batch in student_batches]
            for future in as_completed(futures):
                student_similarities.extend(future.result())
        
        print(f"Found {len(student_similarities)} student-student similarities")
        
        # 3. FIXED STUDENT-TO-ALUMNI SIMILARITIES  
        print("Calculating student-to-alumni similarities...")
        alumni_similarities = []
        
        def process_student_alumni_batch(student_indices):
            batch_similarities = []
            for i in student_indices:
                student = student_list[i]
                candidates = set()
                
                # Debug: Print student info for first few students
                if i < 3:
                    print(f"Processing student {i}: {student['name']}")
                    print(f"Student skills: {student['skills']}")
                    print(f"Student degree: {student['degree']}")
                
                # Find alumni candidates through shared skills
                for skill in student['skills']:
                    if skill.strip():
                        skill_lower = skill.lower()
                        alumni_with_skill = skill_to_alumni.get(skill_lower, [])
                        candidates.update(alumni_with_skill)
                        if i < 3 and alumni_with_skill:
                            print(f"Skill '{skill}' matches {len(alumni_with_skill)} alumni")
                
                # Add through shared degrees
                if student['degree']:
                    degree_lower = student['degree'].lower()
                    alumni_with_degree = degree_to_alumni.get(degree_lower, [])
                    candidates.update(alumni_with_degree)
                    if i < 3 and alumni_with_degree:
                        print(f"Degree '{student['degree']}' matches {len(alumni_with_degree)} alumni")
                
                # Add through shared institutions
                if student.get('institution'):
                    inst_lower = student['institution'].lower()
                    alumni_with_inst = institution_to_alumni.get(inst_lower, [])
                    candidates.update(alumni_with_inst)
                    if i < 3 and alumni_with_inst:
                        print(f"Institution '{student['institution']}' matches {len(alumni_with_inst)} alumni")
                
                if i < 3:
                    print(f"Total alumni candidates for student {i}: {len(candidates)}")
                
                # Calculate similarities
                similarities = []
                for j in candidates:
                    alumni = alumni_list[j]
                    score = self.calculate_similarity_score(
                        student['skills'], student['degree'], student['country'], student.get('institution', ''),
                        alumni['skills'], alumni['degree'], None, alumni.get('institution', '')
                    )
                    if score > 0.05:  # Lower threshold to capture more matches
                        similarities.append((j, score))
                        if i < 3:  # Debug output for first few students
                            print(f"Alumni match: {alumni['name']} with score {score}")
                
                # Keep top 5 similarities (increased from 3)
                similarities.sort(key=lambda x: x[1], reverse=True)
                for similar_idx, score in similarities[:5]:
                    batch_similarities.append({
                        'student': student['name'],
                        'alumni': alumni_list[similar_idx]['name'],
                        'score': score
                    })
            
            return batch_similarities
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_student_alumni_batch, batch) for batch in student_batches]
            for future in as_completed(futures):
                alumni_similarities.extend(future.result())
        
        print(f"Found {len(alumni_similarities)} student-alumni similarities")
        
        # 4. BATCH EXECUTE RELATIONSHIPS
        print("Creating similarity relationships in database...")
        
        # Student-Student relationships
        student_rel_params = []
        for sim in student_similarities:
            student_rel_params.append({
                'student1': sim['student1'],
                'student2': sim['student2'],
                'score': sim['score']
            })
        
        self.batch_execute(
            """
            MATCH (s1:Student {name: $student1})
            MATCH (s2:Student {name: $student2})
            MERGE (s1)-[r:SIMILAR_TO_PEER]->(s2)
            SET r.matched_on = $score
            """,
            student_rel_params,
            "Creating Student-Student similarity relationships"
        )
        
        # Student-Alumni relationships
        alumni_rel_params = []
        for sim in alumni_similarities:
            alumni_rel_params.append({
                'student': sim['student'],
                'alumni': sim['alumni'], 
                'score': sim['score']
            })
        
        self.batch_execute(
            """
            MATCH (s:Student {name: $student})
            MATCH (a:Alumni {name: $alumni})
            MERGE (s)-[r:SIMILAR_TO_ALUMNI]->(a)
            SET r.matched_on = $score
            """,
            alumni_rel_params,
            "Creating Student-Alumni similarity relationships"
        )
    
    def create_similarity_relationships_sampling(self, students_df, alumni_data: Dict, sample_ratio: float = 0.1):
        """Alternative: Create similarity relationships using sampling for very large datasets"""
        start_time = time.time()
        print(f"Creating similarity relationships with {sample_ratio*100}% sampling...")
        
        # Sample students for similarity calculation
        if students_df:
            total_students = students_df.count()
            sample_size = max(1000, int(total_students * sample_ratio))  # At least 1000 or sample_ratio
            
            # Use Spark to sample
            sampled_students_df = students_df.sample(fraction=sample_ratio, seed=42)
            print(f"Sampled {sampled_students_df.count()} out of {total_students} students")
            
            # Process with the sampled dataset
            self.create_similarity_relationships_spark_optimized(sampled_students_df, alumni_data)
        
        print(f"Sampling-based similarity relationships created in {time.time() - start_time:.2f} seconds")
        
        # Prepare alumni data
        alumni_list = []
        for org_data in alumni_data.values():
            for profile in org_data.get('profiles', []):
                skills = set()
                if profile.get('skills'):
                    skills = {s.get('skill', '') for s in profile['skills'] if isinstance(s, dict) and s.get('skill')}
                
                degree = ''
                if profile.get('education'):
                    degrees = [edu.get('degree', '').split(',')[0].strip() for edu in profile['education'] if edu.get('degree')]
                    degree = degrees[0] if degrees else ''
                
                alumni_list.append({
                    'name': profile.get('name', '') or '',
                    'skills': skills,
                    'degree': degree
                })
        
        # Calculate similarities in parallel using ThreadPoolExecutor
        self._calculate_similarities_parallel(student_list, alumni_list)
        
        print(f"Similarity relationships created in {time.time() - start_time:.2f} seconds")
    
    def _calculate_similarities_parallel(self, student_list: List[Dict], alumni_list: List[Dict]):
        """Calculate similarities using parallel processing"""
        def calculate_student_similarities(student_batch):
            similarities = []
            for student in student_batch:
                # Student-to-Student
                student_sims = []
                for other_student in student_list:
                    if student['name'] != other_student['name']:
                        score = self.calculate_similarity_score(
                            student['skills'], student['degree'], student['country'],
                            other_student['skills'], other_student['degree'], other_student['country']
                        )
                        if score > 0:
                            student_sims.append((other_student['name'], score))
                
                student_sims.sort(key=lambda x: x[1], reverse=True)
                for similar_student, score in student_sims[:3]:
                    similarities.append({
                        'query': """
                            MATCH (s1:Student {name: $student1})
                            MATCH (s2:Student {name: $student2})
                            MERGE (s1)-[r:SIMILAR_TO_PEER]->(s2)
                            SET r.matched_on = $score
                        """,
                        'params': {
                            'student1': student['name'],
                            'student2': similar_student,
                            'score': score
                        }
                    })
                
                # Student-to-Alumni
                alumni_sims = []
                for alumni in alumni_list:
                    score = self.calculate_similarity_score(
                        student['skills'], student['degree'], student['country'],
                        alumni['skills'], alumni['degree']
                    )
                    if score > 0:
                        alumni_sims.append((alumni['name'], score))
                
                alumni_sims.sort(key=lambda x: x[1], reverse=True)
                for similar_alumni, score in alumni_sims[:3]:
                    similarities.append({
                        'query': """
                            MATCH (s:Student {name: $student})
                            MATCH (a:Alumni {name: $alumni})
                            MERGE (s)-[r:SIMILAR_TO_ALUMNI]->(a)
                            SET r.matched_on = $score
                        """,
                        'params': {
                            'student': student['name'],
                            'alumni': similar_alumni,
                            'score': score
                        }
                    })
            
            return similarities
        
        # Process students in batches using parallel execution
        batch_size = max(1, len(student_list) // self.max_workers)
        student_batches = [student_list[i:i + batch_size] for i in range(0, len(student_list), batch_size)]
        
        all_similarities = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(calculate_student_similarities, batch) for batch in student_batches]
            for future in as_completed(futures):
                all_similarities.extend(future.result())
        
        # Execute similarity relationships in batches
        with self.driver.session() as session:
            for i in range(0, len(all_similarities), self.batch_size):
                batch = all_similarities[i:i + self.batch_size]
                for sim in batch:
                    try:
                        session.run(sim['query'], **sim['params'])
                    except Exception as e:
                        print(f"Error creating similarity relationship: {e}")
    
    def calculate_similarity_score(self, entity1_skills: Set[str], entity1_degree: str, entity1_country: str, entity1_institution: str,
                             entity2_skills: Set[str], entity2_degree: str, entity2_country: str = None, entity2_institution: str = None) -> float:
        """Calculate similarity score between two entities (updated with institution)"""
        score = 0.0
        
        # Skill similarity (Jaccard coefficient) - weight: 0.4 (reduced to make room for institution)
        if entity1_skills and entity2_skills:
            intersection = len(entity1_skills & entity2_skills)
            union = len(entity1_skills | entity2_skills)
            skill_similarity = intersection / union if union > 0 else 0
            score += skill_similarity * 0.4
        
        # Degree similarity - weight: 0.25
        if entity1_degree and entity2_degree:
            if entity1_degree.lower() == entity2_degree.lower():
                score += 0.25
            elif any(word in entity2_degree.lower() for word in entity1_degree.lower().split() if len(word) > 3):
                score += 0.125
        
        # Institution similarity - weight: 0.25 (NEW)
        if entity1_institution and entity2_institution:
            if entity1_institution.lower() == entity2_institution.lower():
                score += 0.25
        
        # Country similarity - weight: 0.1 (reduced)
        if entity2_country is not None:
            if entity1_country and entity2_country and entity1_country.lower() == entity2_country.lower():
                score += 0.1
        
        return round(score, 3)
    
    def get_graph_statistics(self):
        """Get basic statistics about the created graph"""
        with self.driver.session() as session:
            node_counts = session.run(
                """
                MATCH (n)
                RETURN labels(n)[0] as label, count(n) as count
                ORDER BY count DESC
                """
            ).data()
            
            rel_counts = session.run(
                """
                MATCH ()-[r]->()
                RETURN type(r) as relationship, count(r) as count
                ORDER BY count DESC
                """
            ).data()
            
            print("\n=== GRAPH STATISTICS ===")
            print("\nNode Counts:")
            for item in node_counts:
                print(f"  {item['label']}: {item['count']}")
            
            print("\nRelationship Counts:")
            for item in rel_counts:
                print(f"  {item['relationship']}: {item['count']}")

def main():
    # Neo4j connection details
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USERNAME = "neo4j"
    NEO4J_PASSWORD = "password"
    
    # File paths
    STUDENT_FILE = "../data/graph_data/cleaned_student_profiles.jsonl"
    ALUMNI_FILE = "../data/graph_data/cleaned_alumni_profiles.json"
    
    # Optimization parameters
    BATCH_SIZE = 2000  # Increase batch size for better performance
    MAX_WORKERS = 8    # Adjust based on your CPU cores
    
    # Set random seed for reproducible institution assignments
    random.seed(42)
    
    # Initialize optimized graph builder
    graph_builder = OptimizedNeo4jPropertyGraphBuilder(
        NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, 
        batch_size=BATCH_SIZE, max_workers=MAX_WORKERS
    )
    
    try:
        print("Starting optimized property graph creation...")
        start_time = time.time()
        
        # Clear existing data
        graph_builder.clear_database()
        
        # Create constraints and indexes
        graph_builder.create_constraints_and_indexes()
        
        # Load data with Spark
        students_df, alumni_data = graph_builder.load_data(STUDENT_FILE, ALUMNI_FILE)
        alumni_count = sum(len(org.get('profiles', [])) for org in alumni_data.values())
        student_count = students_df.count() if students_df else 0
        print(f"Loaded {student_count} students and {alumni_count} alumni")
        
        # Extract unique entities with Spark optimization
        entities, alumni_profiles = graph_builder.extract_entities_with_spark(students_df, alumni_data)
        print(f"Extracted entities: {len(entities['countries'])} countries, {len(entities['degrees'])} degrees, "
              f"{len(entities['skills'])} skills, {len(entities['companies'])} companies")
        
        # Create nodes with batch processing
        graph_builder.create_nodes_optimized(entities, students_df, alumni_profiles)
        
        # Create relationships with batch processing
        graph_builder.create_relationships_optimized(students_df, alumni_data)
        
        # Randomly assign alumni skills to 100 students
        graph_builder.randomly_assign_skills_to_students(students_df, alumni_data, 100)
        
        # Create similarity relationships with Spark
        # graph_builder.create_similarity_relationships_spark(students_df, alumni_data)
        graph_builder.create_similarity_relationships_spark_optimized(students_df, alumni_data)

        
        # Get statistics
        graph_builder.get_graph_statistics()
        
        total_time = time.time() - start_time
        print(f"Property graph created successfully in {total_time:.2f} seconds!")
        
    except Exception as e:
        print(f"Error creating property graph: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        graph_builder.close()

if __name__ == "__main__":
    main()
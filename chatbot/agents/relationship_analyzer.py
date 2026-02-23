"""
Relationship Analyzer - World-Class Connection Discovery
Graph-based analysis with intelligent pattern detection
"""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class EntityType(Enum):
    """Types of entities in the relationship graph"""
    PERSON = "person"
    CRIME = "crime"
    LOCATION = "location"
    ORGANIZATION = "organization"
    VEHICLE = "vehicle"
    PHONE = "phone"
    UNKNOWN = "unknown"

class RelationshipType(Enum):
    """Types of relationships between entities"""
    ACCUSED_IN = "accused_in"
    VICTIM_OF = "victim_of"
    WITNESS_TO = "witness_to"
    LOCATED_AT = "located_at"
    CONTACTED_VIA = "contacted_via"
    ASSOCIATED_WITH = "associated_with"
    SIMILAR_TO = "similar_to"

@dataclass
class Entity:
    """Represents an entity in the relationship graph"""
    entity_id: str
    entity_type: EntityType
    attributes: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash(self.entity_id)
    
    def __eq__(self, other):
        return isinstance(other, Entity) and self.entity_id == other.entity_id

@dataclass
class Relationship:
    """Represents a relationship between two entities"""
    source: Entity
    target: Entity
    relationship_type: RelationshipType
    strength: float = 1.0
    evidence: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RelationshipGraph:
    """Graph structure for entities and their relationships"""
    entities: Dict[str, Entity] = field(default_factory=dict)
    relationships: List[Relationship] = field(default_factory=list)
    adjacency: Dict[str, List[Relationship]] = field(default_factory=lambda: defaultdict(list))
    
    def add_entity(self, entity: Entity):
        """Add entity to graph"""
        self.entities[entity.entity_id] = entity
    
    def add_relationship(self, relationship: Relationship):
        """Add relationship to graph"""
        self.relationships.append(relationship)
        self.adjacency[relationship.source.entity_id].append(relationship)
    
    def get_neighbors(self, entity_id: str) -> List[Entity]:
        """Get all entities connected to given entity"""
        neighbors = []
        for rel in self.adjacency.get(entity_id, []):
            neighbors.append(rel.target)
        return neighbors
    
    def find_paths(
        self,
        start_id: str,
        end_id: str,
        max_depth: int = 3
    ) -> List[List[Entity]]:
        """Find all paths between two entities using BFS"""
        if start_id not in self.entities or end_id not in self.entities:
            return []
        
        paths = []
        queue = [([self.entities[start_id]], set([start_id]))]
        
        while queue:
            path, visited = queue.pop(0)
            current = path[-1]
            
            if current.entity_id == end_id:
                paths.append(path)
                continue
            
            if len(path) >= max_depth:
                continue
            
            for neighbor in self.get_neighbors(current.entity_id):
                if neighbor.entity_id not in visited:
                    new_path = path + [neighbor]
                    new_visited = visited | {neighbor.entity_id}
                    queue.append((new_path, new_visited))
        
        return paths
    
    def calculate_centrality(self) -> Dict[str, float]:
        """Calculate degree centrality for each entity"""
        centrality = {}
        for entity_id in self.entities:
            degree = len(self.adjacency.get(entity_id, []))
            centrality[entity_id] = degree
        return centrality
    
    def find_clusters(self) -> List[Set[str]]:
        """Find connected components (clusters) using DFS"""
        visited = set()
        clusters = []
        
        def dfs(entity_id: str, cluster: Set[str]):
            if entity_id in visited:
                return
            visited.add(entity_id)
            cluster.add(entity_id)
            
            for neighbor in self.get_neighbors(entity_id):
                dfs(neighbor.entity_id, cluster)
        
        for entity_id in self.entities:
            if entity_id not in visited:
                cluster = set()
                dfs(entity_id, cluster)
                if len(cluster) > 1:  # Only clusters with multiple entities
                    clusters.append(cluster)
        
        return clusters

@dataclass
class AnalysisResult:
    """Results of relationship analysis"""
    central_entity: Optional[Entity] = None
    graph: RelationshipGraph = field(default_factory=RelationshipGraph)
    patterns: List[str] = field(default_factory=list)
    insights: List[str] = field(default_factory=list)
    key_connections: List[Tuple[Entity, Entity, float]] = field(default_factory=list)
    clusters: List[Set[str]] = field(default_factory=list)
    summary: str = ""

# ============================================================================
# Entity Extractor
# ============================================================================

class EntityExtractor:
    """Extracts entities from raw data"""
    
    @staticmethod
    def extract_from_record(record: Dict[str, Any], source: str = "unknown") -> List[Entity]:
        """Extract all entities from a single record"""
        entities = []
        
        # Extract crime entity
        crime_id = EntityExtractor._get_crime_id(record)
        if crime_id:
            crime = Entity(
                entity_id=f"crime_{crime_id}",
                entity_type=EntityType.CRIME,
                attributes={
                    'crime_id': crime_id,
                    'type': EntityExtractor._get_crime_type(record),
                    'status': EntityExtractor._get_status(record),
                    'date': EntityExtractor._get_date(record)
                },
                metadata={'source': source}
            )
            entities.append(crime)
        
        # Extract person entities
        persons = EntityExtractor._extract_persons(record, source)
        entities.extend(persons)
        
        # Extract location entity
        location = EntityExtractor._extract_location(record, source)
        if location:
            entities.append(location)
        
        # Extract phone entities
        phones = EntityExtractor._extract_phones(record, source)
        entities.extend(phones)
        
        return entities
    
    @staticmethod
    def _get_crime_id(record: Dict) -> Optional[str]:
        """Get crime ID from record"""
        for key in ['crime_id', 'CRIME_ID', '_id', 'id', 'case_number', 'FIR_NUMBER']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_crime_type(record: Dict) -> Optional[str]:
        """Get crime type from record"""
        for key in ['crime_type', 'CRIME_TYPE', 'offense', 'type']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_status(record: Dict) -> Optional[str]:
        """Get status from record"""
        for key in ['status', 'STATUS', 'case_status']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_date(record: Dict) -> Optional[str]:
        """Get date from record"""
        for key in ['date', 'DATE_REGISTERED', 'created_at', 'incident_date']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _extract_persons(record: Dict, source: str) -> List[Entity]:
        """Extract person entities from record"""
        persons = []
        
        # Accused
        for key in ['accused_name', 'ACCUSED_NAME', 'suspect']:
            if key in record and record[key]:
                person = Entity(
                    entity_id=f"person_{record[key]}",
                    entity_type=EntityType.PERSON,
                    attributes={
                        'name': record[key],
                        'role': 'accused'
                    },
                    metadata={'source': source}
                )
                persons.append(person)
        
        # Victim
        for key in ['victim_name', 'VICTIM_NAME', 'complainant']:
            if key in record and record[key]:
                person = Entity(
                    entity_id=f"person_{record[key]}",
                    entity_type=EntityType.PERSON,
                    attributes={
                        'name': record[key],
                        'role': 'victim'
                    },
                    metadata={'source': source}
                )
                persons.append(person)
        
        return persons
    
    @staticmethod
    def _extract_location(record: Dict, source: str) -> Optional[Entity]:
        """Extract location entity from record"""
        for key in ['location', 'LOCATION', 'district', 'DISTRICT', 'place']:
            if key in record and record[key]:
                return Entity(
                    entity_id=f"location_{record[key]}",
                    entity_type=EntityType.LOCATION,
                    attributes={'name': record[key]},
                    metadata={'source': source}
                )
        return None
    
    @staticmethod
    def _extract_phones(record: Dict, source: str) -> List[Entity]:
        """Extract phone number entities from record"""
        phones = []
        for key in ['mobile', 'phone', 'MOBILE_NUMBER', 'contact_number']:
            if key in record and record[key]:
                phone = Entity(
                    entity_id=f"phone_{record[key]}",
                    entity_type=EntityType.PHONE,
                    attributes={'number': record[key]},
                    metadata={'source': source}
                )
                phones.append(phone)
        return phones

# ============================================================================
# Relationship Builder
# ============================================================================

class RelationshipBuilder:
    """Builds relationships between entities"""
    
    @staticmethod
    def build_from_record(
        record: Dict,
        entities: List[Entity]
    ) -> List[Relationship]:
        """Build relationships from a single record"""
        relationships = []
        
        # Find crime entity
        crime = next((e for e in entities if e.entity_type == EntityType.CRIME), None)
        if not crime:
            return relationships
        
        # Connect persons to crime
        for entity in entities:
            if entity.entity_type == EntityType.PERSON:
                role = entity.attributes.get('role', 'unknown')
                
                if role == 'accused':
                    rel_type = RelationshipType.ACCUSED_IN
                elif role == 'victim':
                    rel_type = RelationshipType.VICTIM_OF
                else:
                    rel_type = RelationshipType.ASSOCIATED_WITH
                
                relationship = Relationship(
                    source=entity,
                    target=crime,
                    relationship_type=rel_type,
                    strength=1.0,
                    evidence=[f"From record {crime.attributes.get('crime_id')}"]
                )
                relationships.append(relationship)
        
        # Connect location to crime
        location = next((e for e in entities if e.entity_type == EntityType.LOCATION), None)
        if location:
            relationship = Relationship(
                source=crime,
                target=location,
                relationship_type=RelationshipType.LOCATED_AT,
                strength=1.0,
                evidence=[f"Crime occurred at {location.attributes.get('name')}"]
            )
            relationships.append(relationship)
        
        # Connect phones to persons
        phones = [e for e in entities if e.entity_type == EntityType.PHONE]
        persons = [e for e in entities if e.entity_type == EntityType.PERSON]
        
        for phone in phones:
            for person in persons:
                relationship = Relationship(
                    source=person,
                    target=phone,
                    relationship_type=RelationshipType.CONTACTED_VIA,
                    strength=0.8,
                    evidence=["Contact information"]
                )
                relationships.append(relationship)
        
        return relationships

# ============================================================================
# Pattern Detector
# ============================================================================

class PatternDetector:
    """Detects patterns and insights in relationship graph"""
    
    @staticmethod
    def detect_patterns(graph: RelationshipGraph) -> List[str]:
        """Detect interesting patterns in the graph"""
        patterns = []
        
        # Pattern 1: High-degree entities (connected to many others)
        centrality = graph.calculate_centrality()
        if centrality:
            top_entities = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:3]
            for entity_id, degree in top_entities:
                if degree > 3:
                    entity = graph.entities[entity_id]
                    name = entity.attributes.get('name', entity.entity_id)
                    patterns.append(
                        f"🎯 Central figure: {name} ({entity.entity_type.value}) "
                        f"connected to {degree} other entities"
                    )
        
        # Pattern 2: Clusters (groups of connected entities)
        clusters = graph.find_clusters()
        if len(clusters) > 1:
            patterns.append(f"🔗 Found {len(clusters)} distinct connected groups")
        
        # Pattern 3: Location concentration
        location_crimes = defaultdict(int)
        for rel in graph.relationships:
            if (rel.relationship_type == RelationshipType.LOCATED_AT and
                rel.target.entity_type == EntityType.LOCATION):
                location_crimes[rel.target.attributes.get('name')] += 1
        
        if location_crimes:
            top_location = max(location_crimes.items(), key=lambda x: x[1])
            if top_location[1] > 2:
                patterns.append(
                    f"📍 High concentration at {top_location[0]}: {top_location[1]} incidents"
                )
        
        # Pattern 4: Repeat offenders/victims
        person_counts = defaultdict(lambda: {'accused': 0, 'victim': 0})
        for rel in graph.relationships:
            if rel.source.entity_type == EntityType.PERSON:
                role = rel.source.attributes.get('role')
                name = rel.source.attributes.get('name')
                if role and name:
                    person_counts[name][role] += 1
        
        for name, counts in person_counts.items():
            if counts['accused'] > 1:
                patterns.append(f"⚠️ Repeat accused: {name} ({counts['accused']} cases)")
            if counts['victim'] > 1:
                patterns.append(f"⚠️ Repeat victim: {name} ({counts['victim']} cases)")
        
        return patterns
    
    @staticmethod
    def generate_insights(graph: RelationshipGraph, patterns: List[str]) -> List[str]:
        """Generate actionable insights from patterns"""
        insights = []
        
        # Insight 1: Investigation priorities
        centrality = graph.calculate_centrality()
        if centrality:
            top_entity_id = max(centrality.items(), key=lambda x: x[1])[0]
            entity = graph.entities[top_entity_id]
            if entity.entity_type == EntityType.PERSON:
                insights.append(
                    f"💡 Priority investigation target: {entity.attributes.get('name')} "
                    f"(appears in {centrality[top_entity_id]} cases)"
                )
        
        # Insight 2: Geographic focus
        if any('concentration at' in p for p in patterns):
            insights.append("💡 Consider increased surveillance in high-incident areas")
        
        # Insight 3: Repeat patterns
        if any('Repeat' in p for p in patterns):
            insights.append("💡 Monitor repeat individuals for escalation patterns")
        
        # Insight 4: Network analysis
        clusters = graph.find_clusters()
        if clusters:
            largest_cluster = max(clusters, key=len)
            if len(largest_cluster) > 5:
                insights.append(
                    f"💡 Large criminal network detected ({len(largest_cluster)} entities) "
                    f"- consider coordinated investigation"
                )
        
        return insights

# ============================================================================
# Entity Extractor
# ============================================================================

class EntityExtractor:
    """Extracts entities from raw data"""
    
    @staticmethod
    def extract_from_record(record: Dict[str, Any], source: str = "unknown") -> List[Entity]:
        """Extract all entities from a single record"""
        entities = []
        
        # Extract crime entity
        crime_id = EntityExtractor._get_crime_id(record)
        if crime_id:
            crime = Entity(
                entity_id=f"crime_{crime_id}",
                entity_type=EntityType.CRIME,
                attributes={
                    'crime_id': crime_id,
                    'type': EntityExtractor._get_crime_type(record),
                    'status': EntityExtractor._get_status(record),
                    'date': EntityExtractor._get_date(record)
                },
                metadata={'source': source}
            )
            entities.append(crime)
        
        # Extract person entities
        persons = EntityExtractor._extract_persons(record, source)
        entities.extend(persons)
        
        # Extract location entity
        location = EntityExtractor._extract_location(record, source)
        if location:
            entities.append(location)
        
        # Extract phone entities
        phones = EntityExtractor._extract_phones(record, source)
        entities.extend(phones)
        
        return entities
    
    @staticmethod
    def _get_crime_id(record: Dict) -> Optional[str]:
        """Get crime ID from record"""
        for key in ['crime_id', 'CRIME_ID', '_id', 'id', 'case_number', 'FIR_NUMBER']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_crime_type(record: Dict) -> Optional[str]:
        """Get crime type from record"""
        for key in ['crime_type', 'CRIME_TYPE', 'offense', 'type']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_status(record: Dict) -> Optional[str]:
        """Get status from record"""
        for key in ['status', 'STATUS', 'case_status']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _get_date(record: Dict) -> Optional[str]:
        """Get date from record"""
        for key in ['date', 'DATE_REGISTERED', 'created_at', 'incident_date']:
            if key in record and record[key]:
                return str(record[key])
        return None
    
    @staticmethod
    def _extract_persons(record: Dict, source: str) -> List[Entity]:
        """Extract person entities from record"""
        persons = []
        
        # Accused
        for key in ['accused_name', 'ACCUSED_NAME', 'suspect']:
            if key in record and record[key]:
                person = Entity(
                    entity_id=f"person_{record[key]}",
                    entity_type=EntityType.PERSON,
                    attributes={
                        'name': record[key],
                        'role': 'accused'
                    },
                    metadata={'source': source}
                )
                persons.append(person)
        
        # Victim
        for key in ['victim_name', 'VICTIM_NAME', 'complainant']:
            if key in record and record[key]:
                person = Entity(
                    entity_id=f"person_{record[key]}",
                    entity_type=EntityType.PERSON,
                    attributes={
                        'name': record[key],
                        'role': 'victim'
                    },
                    metadata={'source': source}
                )
                persons.append(person)
        
        return persons
    
    @staticmethod
    def _extract_location(record: Dict, source: str) -> Optional[Entity]:
        """Extract location entity from record"""
        for key in ['location', 'LOCATION', 'district', 'DISTRICT', 'place']:
            if key in record and record[key]:
                return Entity(
                    entity_id=f"location_{record[key]}",
                    entity_type=EntityType.LOCATION,
                    attributes={'name': record[key]},
                    metadata={'source': source}
                )
        return None
    
    @staticmethod
    def _extract_phones(record: Dict, source: str) -> List[Entity]:
        """Extract phone number entities from record"""
        phones = []
        for key in ['mobile', 'phone', 'MOBILE_NUMBER', 'contact_number']:
            if key in record and record[key]:
                phone = Entity(
                    entity_id=f"phone_{record[key]}",
                    entity_type=EntityType.PHONE,
                    attributes={'number': record[key]},
                    metadata={'source': source}
                )
                phones.append(phone)
        return phones

# ============================================================================
# Relationship Builder
# ============================================================================

class RelationshipBuilder:
    """Builds relationships between entities"""
    
    @staticmethod
    def build_from_record(
        record: Dict,
        entities: List[Entity]
    ) -> List[Relationship]:
        """Build relationships from a single record"""
        relationships = []
        
        # Find crime entity
        crime = next((e for e in entities if e.entity_type == EntityType.CRIME), None)
        if not crime:
            return relationships
        
        # Connect persons to crime
        for entity in entities:
            if entity.entity_type == EntityType.PERSON:
                role = entity.attributes.get('role', 'unknown')
                
                if role == 'accused':
                    rel_type = RelationshipType.ACCUSED_IN
                elif role == 'victim':
                    rel_type = RelationshipType.VICTIM_OF
                else:
                    rel_type = RelationshipType.ASSOCIATED_WITH
                
                relationship = Relationship(
                    source=entity,
                    target=crime,
                    relationship_type=rel_type,
                    strength=1.0,
                    evidence=[f"From record {crime.attributes.get('crime_id')}"]
                )
                relationships.append(relationship)
        
        # Connect location to crime
        location = next((e for e in entities if e.entity_type == EntityType.LOCATION), None)
        if location:
            relationship = Relationship(
                source=crime,
                target=location,
                relationship_type=RelationshipType.LOCATED_AT,
                strength=1.0,
                evidence=[f"Crime occurred at {location.attributes.get('name')}"]
            )
            relationships.append(relationship)
        
        # Connect phones to persons
        phones = [e for e in entities if e.entity_type == EntityType.PHONE]
        persons = [e for e in entities if e.entity_type == EntityType.PERSON]
        
        for phone in phones:
            for person in persons:
                relationship = Relationship(
                    source=person,
                    target=phone,
                    relationship_type=RelationshipType.CONTACTED_VIA,
                    strength=0.8,
                    evidence=["Contact information"]
                )
                relationships.append(relationship)
        
        return relationships

# ============================================================================
# Pattern Detector
# ============================================================================

class PatternDetector:
    """Detects patterns and insights in relationship graph"""
    
    @staticmethod
    def detect_patterns(graph: RelationshipGraph) -> List[str]:
        """Detect interesting patterns in the graph"""
        patterns = []
        
        # Pattern 1: High-degree entities (connected to many others)
        centrality = graph.calculate_centrality()
        if centrality:
            top_entities = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:3]
            for entity_id, degree in top_entities:
                if degree > 3:
                    entity = graph.entities[entity_id]
                    name = entity.attributes.get('name', entity.entity_id)
                    patterns.append(
                        f"🎯 Central figure: {name} ({entity.entity_type.value}) "
                        f"connected to {degree} other entities"
                    )
        
        # Pattern 2: Clusters (groups of connected entities)
        clusters = graph.find_clusters()
        if len(clusters) > 1:
            patterns.append(f"🔗 Found {len(clusters)} distinct connected groups")
        
        # Pattern 3: Location concentration
        location_crimes = defaultdict(int)
        for rel in graph.relationships:
            if (rel.relationship_type == RelationshipType.LOCATED_AT and
                rel.target.entity_type == EntityType.LOCATION):
                location_crimes[rel.target.attributes.get('name')] += 1
        
        if location_crimes:
            top_location = max(location_crimes.items(), key=lambda x: x[1])
            if top_location[1] > 2:
                patterns.append(
                    f"📍 High concentration at {top_location[0]}: {top_location[1]} incidents"
                )
        
        # Pattern 4: Repeat offenders/victims
        person_counts = defaultdict(lambda: {'accused': 0, 'victim': 0})
        for rel in graph.relationships:
            if rel.source.entity_type == EntityType.PERSON:
                role = rel.source.attributes.get('role')
                name = rel.source.attributes.get('name')
                if role and name:
                    person_counts[name][role] += 1
        
        for name, counts in person_counts.items():
            if counts['accused'] > 1:
                patterns.append(f"⚠️ Repeat accused: {name} ({counts['accused']} cases)")
            if counts['victim'] > 1:
                patterns.append(f"⚠️ Repeat victim: {name} ({counts['victim']} cases)")
        
        return patterns
    
    @staticmethod
    def generate_insights(graph: RelationshipGraph, patterns: List[str]) -> List[str]:
        """Generate actionable insights from patterns"""
        insights = []
        
        # Insight 1: Investigation priorities
        centrality = graph.calculate_centrality()
        if centrality:
            top_entity_id = max(centrality.items(), key=lambda x: x[1])[0]
            entity = graph.entities[top_entity_id]
            if entity.entity_type == EntityType.PERSON:
                insights.append(
                    f"💡 Priority investigation target: {entity.attributes.get('name')} "
                    f"(appears in {centrality[top_entity_id]} cases)"
                )
        
        # Insight 2: Geographic focus
        if any('concentration at' in p for p in patterns):
            insights.append("💡 Consider increased surveillance in high-incident areas")
        
        # Insight 3: Repeat patterns
        if any('Repeat' in p for p in patterns):
            insights.append("💡 Monitor repeat individuals for escalation patterns")
        
        # Insight 4: Network analysis
        clusters = graph.find_clusters()
        if clusters:
            largest_cluster = max(clusters, key=len)
            if len(largest_cluster) > 5:
                insights.append(
                    f"💡 Large criminal network detected ({len(largest_cluster)} entities) "
                    f"- consider coordinated investigation"
                )
        
        return insights

# ============================================================================
# Main Analyzer Class
# ============================================================================

class RelationshipAnalyzer:
    """
    World-class relationship analyzer with graph-based intelligence
    """
    
    def __init__(self):
        self.extractor = EntityExtractor()
        self.builder = RelationshipBuilder()
        self.pattern_detector = PatternDetector()
    
    def analyze_crime_relationships(
        self,
        v1_data: List[Dict],
        v2_data: List[Dict],
        central_entity_id: Optional[str] = None
    ) -> AnalysisResult:
        """
        Analyze relationships in crime data with graph algorithms
        
        Args:
            v1_data: MongoDB results
            v2_data: PostgreSQL results
            central_entity_id: Optional ID of central entity to focus on
        
        Returns:
            Complete analysis with graph, patterns, and insights
        """
        result = AnalysisResult()
        graph = RelationshipGraph()
        
        # Process V2 Data (PostgreSQL)
        for record in v2_data:
            entities = self.extractor.extract_from_record(record, source='v2_data')
            for entity in entities:
                graph.add_entity(entity)
            
            relationships = self.builder.build_from_record(record, entities)
            for rel in relationships:
                graph.add_relationship(rel)
        
        # Process V1 Data (MongoDB)
        for record in v1_data:
            entities = self.extractor.extract_from_record(record, source='v1_data')
            for entity in entities:
                graph.add_entity(entity)
            
            relationships = self.builder.build_from_record(record, entities)
            for rel in relationships:
                graph.add_relationship(rel)
        
        result.graph = graph
        
        # Detect patterns
        result.patterns = self.pattern_detector.detect_patterns(graph)
        
        # Generate insights
        result.insights = self.pattern_detector.generate_insights(graph, result.patterns)
        
        # Find key connections
        result.key_connections = self._find_key_connections(graph)
        
        # Find clusters
        result.clusters = graph.find_clusters()
        
        # Set central entity
        if central_entity_id and central_entity_id in graph.entities:
            result.central_entity = graph.entities[central_entity_id]
        elif graph.entities:
            # Find most central entity
            centrality = graph.calculate_centrality()
            if centrality:
                central_id = max(centrality.items(), key=lambda x: x[1])[0]
                result.central_entity = graph.entities[central_id]
        
        # Generate summary
        result.summary = self._generate_summary(result)
        
        logger.info(f"Analysis complete: {len(graph.entities)} entities, "
                   f"{len(graph.relationships)} relationships, "
                   f"{len(result.patterns)} patterns detected")
        
        return result
    
    def _find_key_connections(
        self,
        graph: RelationshipGraph,
        top_n: int = 5
    ) -> List[Tuple[Entity, Entity, float]]:
        """Find most important connections in graph"""
        scored_rels = []
        
        for rel in graph.relationships:
            score = rel.strength
            
            # Boost score for person-crime relationships
            if (rel.source.entity_type == EntityType.PERSON and
                rel.target.entity_type == EntityType.CRIME):
                score *= 1.5
            
            # Boost score for accused relationships
            if rel.relationship_type == RelationshipType.ACCUSED_IN:
                score *= 2.0
            
            scored_rels.append((rel.source, rel.target, score))
        
        # Sort by score and return top N
        scored_rels.sort(key=lambda x: x[2], reverse=True)
        return scored_rels[:top_n]
    
    def _generate_summary(self, result: AnalysisResult) -> str:
        """Generate human-readable summary"""
        parts = []
        
        entity_count = len(result.graph.entities)
        rel_count = len(result.graph.relationships)
        
        parts.append(f"Network analysis: {entity_count} entities, {rel_count} relationships")
        
        if result.patterns:
            parts.append(f"{len(result.patterns)} patterns detected")
        
        if result.clusters:
            parts.append(f"{len(result.clusters)} distinct groups identified")
        
        return ". ".join(parts)
    
    def extract_person_details(self, data: List[Dict]) -> Dict[str, Any]:
        """Extract comprehensive person details from data"""
        person_info = {
            'names': set(),
            'mobile_numbers': set(),
            'emails': set(),
            'addresses': set(),
            'roles': set(),
            'associated_crimes': [],
            'identity_docs': {}
        }
        
        for record in data:
            # Names
            for key in ['name', 'accused_name', 'victim_name', 'ACCUSED_NAME', 'VICTIM_NAME']:
                if key in record and record[key]:
                    person_info['names'].add(str(record[key]))
                    
                    # Track role
                    if 'accused' in key.lower():
                        person_info['roles'].add('Accused')
                    elif 'victim' in key.lower():
                        person_info['roles'].add('Victim')
            
            # Contact info
            for key in ['mobile', 'phone', 'MOBILE_NUMBER', 'contact_number']:
                if key in record and record[key]:
                    person_info['mobile_numbers'].add(str(record[key]))
            
            for key in ['email', 'EMAIL']:
                if key in record and record[key]:
                    person_info['emails'].add(str(record[key]))
            
            for key in ['address', 'ADDRESS', 'location', 'LOCATION']:
                if key in record and record[key]:
                    person_info['addresses'].add(str(record[key]))
            
            # Identity documents
            id_fields = {
                'aadhaar': ['aadhaar', 'AADHAAR', 'aadhaar_number', 'AADHAAR_NUMBER'],
                'pan': ['pan', 'PAN', 'pan_number', 'PAN_NUMBER'],
                'passport': ['passport', 'PASSPORT', 'passport_number'],
                'voter_id': ['voter_id', 'VOTER_ID', 'epic_no'],
                'driving_license': ['driving_license', 'DL_NUMBER', 'dl_number']
            }
            
            for doc_type, keys in id_fields.items():
                for key in keys:
                    if key in record and record[key]:
                        person_info['identity_docs'][doc_type] = str(record[key])
                        break
            
            # Associated crimes
            crime_id = self.extractor._get_crime_id(record)
            if crime_id:
                person_info['associated_crimes'].append(crime_id)
        
        # Convert sets to sorted lists
        return {
            'names': sorted(list(person_info['names'])),
            'mobile_numbers': sorted(list(person_info['mobile_numbers'])),
            'emails': sorted(list(person_info['emails'])),
            'addresses': sorted(list(person_info['addresses'])),
            'roles': sorted(list(person_info['roles'])),
            'crime_count': len(person_info['associated_crimes']),
            'identity_docs': person_info['identity_docs']
        }


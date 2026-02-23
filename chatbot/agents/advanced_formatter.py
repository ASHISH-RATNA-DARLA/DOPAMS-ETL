"""
Advanced Formatter - World-Class Result Presentation
Beautiful, intelligent, and highly customizable data formatting
"""

import logging
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from functools import lru_cache

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration & Themes
# ============================================================================

class FormatterTheme(Enum):
    """Visual themes for output"""
    DEFAULT = "default"
    MINIMAL = "minimal"
    DETAILED = "detailed"
    POLICE_REPORT = "police_report"
    EXECUTIVE = "executive"

@dataclass
class FormatterConfig:
    """Configuration for formatter behavior"""
    theme: FormatterTheme = FormatterTheme.DEFAULT
    max_records_display: int = 5
    max_field_length: int = 100
    show_statistics: bool = True
    show_timestamps: bool = True
    use_emojis: bool = True
    truncate_long_text: bool = True
    highlight_important_fields: bool = True
    custom_field_labels: Dict[str, str] = field(default_factory=dict)
    
    # Field priorities for display order
    priority_fields: List[str] = field(default_factory=lambda: [
        'id', 'crime_id', 'case_number', 'name', 'status',
        'date', 'type', 'location', 'severity'
    ])

# ============================================================================
# Field Mappings & Aliases
# ============================================================================

class FieldMapper:
    """Maps various field names to standard labels"""
    
    FIELD_ALIASES = {
        # IDs
        'crime_id': ['CRIME_ID', '_id', 'id', 'case_id'],
        'case_number': ['CASE_NUMBER', 'case_no', 'FIR_NUMBER', 'fir_no'],
        
        # Person info
        'name': ['NAME', 'accused_name', 'ACCUSED_NAME', 'victim_name', 'VICTIM_NAME'],
        'mobile': ['mobile', 'MOBILE_NUMBER', 'phone', 'PHONE', 'contact_number'],
        'email': ['email', 'EMAIL', 'email_address'],
        'address': ['address', 'ADDRESS', 'location', 'LOCATION'],
        'aadhaar': ['aadhaar', 'AADHAAR', 'aadhaar_number', 'AADHAAR_NUMBER', 'aadhar'],
        'pan': ['pan', 'PAN', 'pan_number', 'PAN_NUMBER'],
        
        # Crime details
        'crime_type': ['CRIME_TYPE', 'offense', 'type', 'offense_type'],
        'status': ['STATUS', 'case_status', 'investigation_status'],
        'date': ['date', 'DATE_REGISTERED', 'created_at', 'registered_date', 'incident_date'],
        'location': ['location', 'LOCATION', 'place', 'DISTRICT', 'district'],
        
        # Additional
        'description': ['description', 'DESCRIPTION', 'details', 'notes'],
        'officer': ['officer', 'investigating_officer', 'IO_NAME']
    }
    
    DISPLAY_LABELS = {
        'crime_id': '🆔 Case ID',
        'case_number': '📋 Case Number',
        'name': '👤 Name',
        'mobile': '📱 Mobile',
        'email': '📧 Email',
        'address': '📍 Address',
        'aadhaar': '🆔 Aadhaar',
        'pan': '💳 PAN',
        'crime_type': '⚖️ Crime Type',
        'status': '📊 Status',
        'date': '📅 Date',
        'location': '🗺️ Location',
        'description': '📝 Description',
        'officer': '👮 Officer'
    }
    
    @classmethod
    def get_standard_field(cls, data: Dict, field_aliases: List[str]) -> Optional[Any]:
        """Get field value using multiple possible key names"""
        for alias in field_aliases:
            if alias in data and data[alias]:
                return data[alias]
        return None
    
    @classmethod
    def get_display_label(cls, field_name: str, use_emoji: bool = True) -> str:
        """Get pretty display label for field"""
        label = cls.DISPLAY_LABELS.get(field_name, field_name.replace('_', ' ').title())
        if not use_emoji:
            # Remove emoji from label
            label = ''.join(c for c in label if ord(c) < 128)
        return label

# ============================================================================
# Statistics Calculator
# ============================================================================

class StatisticsCalculator:
    """Calculate intelligent statistics from data"""
    
    @staticmethod
    def calculate_crime_statistics(records: List[Dict]) -> Dict[str, Any]:
        """Calculate comprehensive crime statistics"""
        if not records:
            return {}
        
        stats = {
            'total_count': len(records),
            'crime_types': {},
            'statuses': {},
            'locations': {},
            'date_range': {'earliest': None, 'latest': None},
            'top_patterns': []
        }
        
        for record in records:
            # Crime types distribution
            crime_type = FieldMapper.get_standard_field(
                record,
                FieldMapper.FIELD_ALIASES['crime_type']
            )
            if crime_type:
                stats['crime_types'][str(crime_type)] = \
                    stats['crime_types'].get(str(crime_type), 0) + 1
            
            # Status distribution
            status = FieldMapper.get_standard_field(
                record,
                FieldMapper.FIELD_ALIASES['status']
            )
            if status:
                stats['statuses'][str(status)] = \
                    stats['statuses'].get(str(status), 0) + 1
            
            # Location distribution
            location = FieldMapper.get_standard_field(
                record,
                FieldMapper.FIELD_ALIASES['location']
            )
            if location:
                loc_str = str(location)[:50]  # Truncate long locations
                stats['locations'][loc_str] = \
                    stats['locations'].get(loc_str, 0) + 1
            
            # Date range
            date = FieldMapper.get_standard_field(
                record,
                FieldMapper.FIELD_ALIASES['date']
            )
            if date:
                date_str = str(date)
                if not stats['date_range']['earliest'] or date_str < stats['date_range']['earliest']:
                    stats['date_range']['earliest'] = date_str
                if not stats['date_range']['latest'] or date_str > stats['date_range']['latest']:
                    stats['date_range']['latest'] = date_str
        
        # Sort by frequency
        stats['crime_types'] = dict(sorted(
            stats['crime_types'].items(),
            key=lambda x: x[1],
            reverse=True
        ))
        stats['locations'] = dict(sorted(
            stats['locations'].items(),
            key=lambda x: x[1],
            reverse=True
        ))
        
        return stats
    
    @staticmethod
    def identify_patterns(records: List[Dict]) -> List[str]:
        """Identify interesting patterns in data"""
        patterns = []
        
        if not records:
            return patterns
        
        stats = StatisticsCalculator.calculate_crime_statistics(records)
        
        # High concentration in single location
        if stats['locations']:
            top_location = list(stats['locations'].items())[0]
            if top_location[1] / len(records) > 0.5:
                patterns.append(
                    f"High concentration in {top_location[0]} "
                    f"({top_location[1]}/{len(records)} cases)"
                )
        
        # Dominant crime type
        if stats['crime_types']:
            top_crime = list(stats['crime_types'].items())[0]
            if top_crime[1] / len(records) > 0.6:
                patterns.append(
                    f"Predominant crime type: {top_crime[0]} "
                    f"({top_crime[1]}/{len(records)} cases)"
                )
        
        # Status distribution insights
        if stats['statuses']:
            if 'PENDING' in stats['statuses'] or 'Open' in stats['statuses']:
                pending = stats['statuses'].get('PENDING', 0) + stats['statuses'].get('Open', 0)
                if pending / len(records) > 0.7:
                    patterns.append(f"⚠️ {pending} cases still pending resolution")
        
        return patterns

# ============================================================================
# Template Engine
# ============================================================================

class TemplateEngine:
    """Render data using templates"""
    
    @staticmethod
    def render_person_profile(
        person_data: Dict[str, Any],
        crime_data: List[Dict],
        config: FormatterConfig
    ) -> str:
        """Render person profile with template"""
        sections = []
        
        # Header
        names = person_data.get('names', [])
        name = names[0] if names else 'Unknown Person'
        
        if config.theme == FormatterTheme.POLICE_REPORT:
            sections.append("=" * 60)
            sections.append(f"PERSON PROFILE REPORT")
            sections.append(f"Subject: {name.upper()}")
            sections.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            sections.append("=" * 60)
        else:
            emoji = "👤 " if config.use_emojis else ""
            sections.append(f"## {emoji}Person Profile: {name}")
        
        sections.append("")
        
        # Identity Documents Section (NEW!)
        identity_section = TemplateEngine._render_identity_section(person_data, config)
        if identity_section:
            sections.append(identity_section)
        
        # Contact Information
        sections.append(TemplateEngine._render_contact_section(person_data, config))
        
        # Related Crimes
        sections.append(TemplateEngine._render_crimes_section(crime_data, config))
        
        # Statistics (if enabled)
        if config.show_statistics and crime_data:
            sections.append(TemplateEngine._render_statistics_section(crime_data, config))
        
        return "\n".join(sections)
    
    @staticmethod
    def _render_identity_section(person_data: Dict, config: FormatterConfig) -> str:
        """Render identity documents section"""
        lines = []
        
        # Check if any identity data exists
        has_identity = any(k in person_data for k in ['aadhaar', 'pan', 'passport', 'voter_id', 'driving_license'])
        
        if not has_identity:
            return ""
        
        emoji = "🆔 " if config.use_emojis else ""
        lines.append(f"### {emoji}Identity Documents")
        
        # Show identity documents if available
        identity_fields = {
            'aadhaar': ('🆔', 'Aadhaar'),
            'pan': ('💳', 'PAN'),
            'passport': ('✈️', 'Passport'),
            'voter_id': ('🗳️', 'Voter ID'),
            'driving_license': ('🚗', 'Driving License')
        }
        
        for field_key, (emoji_icon, label) in identity_fields.items():
            if field_key in person_data and person_data[field_key]:
                emoji_str = f"{emoji_icon} " if config.use_emojis else ""
                lines.append(f"• {emoji_str}**{label}:** {person_data[field_key]}")
        
        lines.append("")
        return "\n".join(lines)
    
    @staticmethod
    def _render_contact_section(person_data: Dict, config: FormatterConfig) -> str:
        """Render contact information section"""
        lines = []
        
        emoji_prefix = "📞 " if config.use_emojis else ""
        lines.append(f"### {emoji_prefix}Contact Information")
        
        # Mobile numbers
        if person_data.get('mobile_numbers'):
            for mobile in person_data['mobile_numbers'][:3]:  # Limit to 3
                emoji = "📱 " if config.use_emojis else ""
                lines.append(f"• {emoji}**Mobile:** {mobile}")
        
        # Emails
        if person_data.get('emails'):
            for email in person_data['emails'][:3]:
                emoji = "📧 " if config.use_emojis else ""
                lines.append(f"• {emoji}**Email:** {email}")
        
        # Addresses
        if person_data.get('addresses'):
            for addr in person_data['addresses'][:2]:
                emoji = "📍 " if config.use_emojis else ""
                addr_display = addr[:80] + "..." if len(addr) > 80 else addr
                lines.append(f"• {emoji}**Address:** {addr_display}")
        
        if not (person_data.get('mobile_numbers') or person_data.get('emails') or person_data.get('addresses')):
            lines.append("No contact information available.")
        
        lines.append("")
        return "\n".join(lines)
    
    @staticmethod
    def _render_crimes_section(crime_data: List[Dict], config: FormatterConfig) -> str:
        """Render crimes section"""
        lines = []
        
        emoji = "🚨 " if config.use_emojis else ""
        lines.append(f"### {emoji}Related Crimes ({len(crime_data)})")
        
        if not crime_data:
            lines.append("No crime records found.")
            lines.append("")
            return "\n".join(lines)
        
        # Display records
        for i, crime in enumerate(crime_data[:config.max_records_display], 1):
            lines.append(f"\n**Crime {i}:**")
            
            # Get important fields using FieldMapper
            crime_id = FieldMapper.get_standard_field(crime, FieldMapper.FIELD_ALIASES['crime_id'])
            crime_type = FieldMapper.get_standard_field(crime, FieldMapper.FIELD_ALIASES['crime_type'])
            status = FieldMapper.get_standard_field(crime, FieldMapper.FIELD_ALIASES['status'])
            date = FieldMapper.get_standard_field(crime, FieldMapper.FIELD_ALIASES['date'])
            location = FieldMapper.get_standard_field(crime, FieldMapper.FIELD_ALIASES['location'])
            
            if crime_id:
                lines.append(f"  • **Case ID:** {crime_id}")
            if crime_type:
                lines.append(f"  • **Type:** {crime_type}")
            if status:
                lines.append(f"  • **Status:** {status}")
            if date:
                lines.append(f"  • **Date:** {date}")
            if location:
                loc_display = str(location)[:60]
                lines.append(f"  • **Location:** {loc_display}")
        
        if len(crime_data) > config.max_records_display:
            lines.append(f"\n_... and {len(crime_data) - config.max_records_display} more crimes_")
        
        lines.append("")
        return "\n".join(lines)
    
    @staticmethod
    def _render_statistics_section(crime_data: List[Dict], config: FormatterConfig) -> str:
        """Render statistics section"""
        lines = []
        
        stats = StatisticsCalculator.calculate_crime_statistics(crime_data)
        patterns = StatisticsCalculator.identify_patterns(crime_data)
        
        emoji = "📊 " if config.use_emojis else ""
        lines.append(f"### {emoji}Analysis & Insights")
        lines.append("")
        
        # Crime type breakdown
        if stats['crime_types']:
            lines.append("**Crime Type Distribution:**")
            for crime_type, count in list(stats['crime_types'].items())[:5]:
                percentage = (count / stats['total_count']) * 100
                lines.append(f"  • {crime_type}: {count} ({percentage:.1f}%)")
            lines.append("")
        
        # Date range
        if stats['date_range']['earliest'] and stats['date_range']['latest']:
            lines.append(f"**Date Range:** {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            lines.append("")
        
        # Patterns & Insights
        if patterns:
            lines.append("**Key Insights:**")
            for pattern in patterns:
                lines.append(f"  • {pattern}")
            lines.append("")
        
        return "\n".join(lines)

# ============================================================================
# Main Formatter Class
# ============================================================================

class AdvancedFormatter:
    """
    World-class data formatter with intelligence and beauty
    """
    
    def __init__(self, config: Optional[FormatterConfig] = None):
        """
        Initialize formatter
        
        Args:
            config: Optional configuration (uses defaults if None)
        """
        self.config = config or FormatterConfig()
        self.template_engine = TemplateEngine()
        self.stats_calculator = StatisticsCalculator()
    
    def format_person_profile(
        self,
        person_data: Dict[str, Any],
        crime_data: List[Dict]
    ) -> str:
        """
        Format complete person profile with related crimes
        
        Args:
            person_data: Person information (names, contacts, etc.)
            crime_data: Related crime records
        
        Returns:
            Beautifully formatted profile
        """
        return self.template_engine.render_person_profile(
            person_data,
            crime_data,
            self.config
        )
    
    def format_crime_summary(
        self,
        crime_records: List[Dict],
        entity_value: Optional[str] = None
    ) -> str:
        """
        Format crime summary with intelligent analysis
        
        Args:
            crime_records: List of crime records
            entity_value: Optional search entity for context
        
        Returns:
            Beautifully formatted summary with statistics
        """
        if not crime_records:
            emoji = "❌ " if self.config.use_emojis else ""
            return f"{emoji}No crime records found."
        
        sections = []
        count = len(crime_records)
        
        # Header
        emoji = "📊 " if self.config.use_emojis else ""
        plural = 's' if count != 1 else ''
        sections.append(f"## {emoji}Crime Analysis ({count} record{plural})")
        sections.append("")
        
        # Statistics
        if self.config.show_statistics:
            stats = self.stats_calculator.calculate_crime_statistics(crime_records)
            sections.append(self._format_statistics(stats))
        
        # Detailed records
        emoji = "📋 " if self.config.use_emojis else ""
        sections.append(f"### {emoji}Detailed Records")
        sections.append("")
        
        sections.append(self._format_records(crime_records))
        
        return "\n".join(sections)
    
    def format_data_summary(
        self,
        v1_data: List[Dict],
        v2_data: List[Dict],
        query_context: str = ""
    ) -> str:
        """
        Create intelligent summary of combined data sources
        
        Args:
            v1_data: MongoDB results
            v2_data: PostgreSQL results
            query_context: User's original query for context
        
        Returns:
            Beautifully formatted combined summary
        """
        total = len(v1_data) + len(v2_data)
        
        if total == 0:
            emoji = "❌ " if self.config.use_emojis else ""
            return f"{emoji}No matching records found in either data source."
        
        sections = []
        
        # Header
        emoji = "📊 " if self.config.use_emojis else ""
        sections.append(f"## {emoji}Combined Data Analysis ({total} total records)")
        sections.append("")
        
        # V2 Data (PostgreSQL)
        if v2_data:
            emoji = "🗄️ " if self.config.use_emojis else ""
            sections.append(f"### {emoji}V2 Data ({len(v2_data)} records)")
            sections.append("")
            sections.append(self._format_records(v2_data[:self.config.max_records_display]))
            
            if len(v2_data) > self.config.max_records_display:
                sections.append(f"\n_... and {len(v2_data) - self.config.max_records_display} more records_")
            sections.append("")
        
        # V1 Data (MongoDB)
        if v1_data:
            emoji = "📚 " if self.config.use_emojis else ""
            sections.append(f"### {emoji}V1 Data ({len(v1_data)} documents)")
            sections.append("")
            sections.append(self._format_records(v1_data[:self.config.max_records_display]))
            
            if len(v1_data) > self.config.max_records_display:
                sections.append(f"\n_... and {len(v1_data) - self.config.max_records_display} more documents_")
        
        return "\n".join(sections)
    
    def _format_statistics(self, stats: Dict[str, Any]) -> str:
        """Format statistics section"""
        lines = []
        
        emoji = "📈 " if self.config.use_emojis else ""
        lines.append(f"### {emoji}Summary Statistics")
        lines.append("")
        lines.append(f"• **Total Records:** {stats['total_count']}")
        
        # Crime types
        if stats['crime_types']:
            top_crimes = list(stats['crime_types'].items())[:3]
            crime_str = ', '.join(f"{k} ({v})" for k, v in top_crimes)
            lines.append(f"• **Crime Types:** {crime_str}")
        
        # Statuses
        if stats['statuses']:
            status_str = ', '.join(f"{k} ({v})" for k, v in stats['statuses'].items())
            lines.append(f"• **Status Distribution:** {status_str}")
        
        # Top location
        if stats['locations']:
            top_loc = list(stats['locations'].items())[0]
            lines.append(f"• **Top Location:** {top_loc[0]} ({top_loc[1]} cases)")
        
        lines.append("")
        return "\n".join(lines)
    
    def _format_records(self, records: List[Dict]) -> str:
        """Format individual records with intelligent field selection"""
        lines = []
        
        for i, record in enumerate(records[:self.config.max_records_display], 1):
            lines.append(f"**Record {i}:**")
            
            # Get prioritized fields
            displayed_fields = set()
            
            # Show priority fields first
            for std_field in self.config.priority_fields:
                if std_field in FieldMapper.FIELD_ALIASES:
                    value = FieldMapper.get_standard_field(
                        record,
                        FieldMapper.FIELD_ALIASES[std_field]
                    )
                    if value:
                        label = FieldMapper.get_display_label(std_field, self.config.use_emojis)
                        value_str = self._format_value(value)
                        lines.append(f"  • {label}: {value_str}")
                        # Track which keys we've displayed
                        for alias in FieldMapper.FIELD_ALIASES[std_field]:
                            displayed_fields.add(alias)
            
            # Count remaining fields
            remaining = len([k for k in record.keys() if k not in displayed_fields])
            if remaining > 0:
                lines.append(f"  • _... and {remaining} more fields_")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _format_value(self, value: Any) -> str:
        """Format a single value for display"""
        if value is None:
            return "—"
        
        value_str = str(value)
        
        # Truncate if needed
        if self.config.truncate_long_text and len(value_str) > self.config.max_field_length:
            value_str = value_str[:self.config.max_field_length] + "..."
        
        # Format numbers with commas
        if isinstance(value, (int, float)) and value > 1000:
            try:
                value_str = f"{value:,}"
            except:
                pass
        
        return value_str
    
    def create_visual_chart(
        self,
        data: Dict[str, int],
        title: str = "Distribution",
        max_bar_length: int = 40
    ) -> str:
        """
        Create ASCII bar chart
        
        Args:
            data: Dict of {label: count}
            title: Chart title
            max_bar_length: Maximum bar length in characters
        
        Returns:
            ASCII bar chart
        """
        if not data:
            return ""
        
        lines = []
        emoji = "📊 " if self.config.use_emojis else ""
        lines.append(f"### {emoji}{title}")
        lines.append("")
        lines.append("```")
        
        # Find max value for scaling
        max_value = max(data.values()) if data else 1
        
        for label, count in sorted(data.items(), key=lambda x: x[1], reverse=True)[:10]:
            # Calculate bar length
            bar_len = int((count / max_value) * max_bar_length) if max_value > 0 else 0
            bar = "█" * bar_len
            
            # Format label (truncate if needed)
            label_str = label[:20].ljust(20)
            
            lines.append(f"{label_str} | {bar} {count}")
        
        lines.append("```")
        lines.append("")
        
        return "\n".join(lines)

# ============================================================================
# Factory Functions
# ============================================================================

def create_formatter(
    theme: FormatterTheme = FormatterTheme.DEFAULT,
    max_records: int = 5,
    use_emojis: bool = True,
    show_stats: bool = True
) -> AdvancedFormatter:
    """
    Factory function to create configured formatter
    
    Args:
        theme: Visual theme
        max_records: Maximum records to display
        use_emojis: Whether to use emojis in output
        show_stats: Whether to show statistics
    
    Returns:
        Configured AdvancedFormatter
    """
    config = FormatterConfig(
        theme=theme,
        max_records_display=max_records,
        use_emojis=use_emojis,
        show_statistics=show_stats
    )
    return AdvancedFormatter(config)


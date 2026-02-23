"""
Date utility functions for chunking with overlap
"""
from datetime import datetime, timedelta
from typing import List, Tuple


def generate_date_chunks(start_date: str, end_date: str = None, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
    """
    Generate date chunks with overlap to avoid missing data.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (default: today)
        chunk_days: Number of days per chunk (default: 5)
        overlap_days: Number of days to overlap (default: 1)
    
    Returns:
        List of tuples: [(from_date, to_date), ...]
    
    Example:
        start_date='2022-01-01', chunk_days=5, overlap_days=1
        Returns: [
            ('2022-01-01', '2022-01-05'),
            ('2022-01-04', '2022-01-09'),  # 1 day overlap
            ('2022-01-08', '2022-01-13'),
            ...
        ]
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")
    
    chunks = []
    current_start = start
    
    while current_start <= end:
        # Calculate chunk end date
        chunk_end = current_start + timedelta(days=chunk_days - 1)
        
        # Don't go beyond end_date
        if chunk_end > end:
            chunk_end = end
        
        chunks.append((
            current_start.strftime('%Y-%m-%d'),
            chunk_end.strftime('%Y-%m-%d')
        ))
        
        # Move to next chunk with overlap
        # Next start = current_end - overlap_days + 1
        current_start = chunk_end - timedelta(days=overlap_days - 1)
        
        # If we've reached the end, break
        if chunk_end >= end:
            break
    
    return chunks


def generate_date_chunks_backwards(end_date: str = None, start_date: str = None, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
    """
    Generate date chunks going BACKWARDS from end_date to start_date.
    This processes the most recent data first.
    
    Args:
        end_date: End date in YYYY-MM-DD format (default: today) - this is where we START processing
        start_date: Start date in YYYY-MM-DD format (default: 2022-01-01) - this is where we STOP
        chunk_days: Number of days per chunk (default: 5)
        overlap_days: Number of days to overlap (default: 1)
    
    Returns:
        List of tuples: [(from_date, to_date), ...] going backwards
        First chunk is most recent, last chunk is oldest
    
    Example:
        end_date='2025-01-26', start_date='2022-01-01', chunk_days=5, overlap_days=1
        Returns: [
            ('2025-01-22', '2025-01-26'),  # Most recent first
            ('2025-01-18', '2025-01-22'),  # 1 day overlap
            ('2025-01-14', '2025-01-18'),
            ...
            ('2022-01-01', '2022-01-05'),  # Oldest last
        ]
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if start_date is None:
        start_date = '2022-01-01'
    
    end = datetime.strptime(end_date, '%Y-%m-%d')
    start = datetime.strptime(start_date, '%Y-%m-%d')
    
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")
    
    chunks = []
    current_end = end
    
    while current_end >= start:
        # Calculate chunk start date (going backwards)
        chunk_start = current_end - timedelta(days=chunk_days - 1)
        
        # Don't go before start_date
        if chunk_start < start:
            chunk_start = start
        
        chunks.append((
            chunk_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        
        # Move to next chunk backwards with overlap
        # Next end = current_start - overlap_days
        current_end = chunk_start - timedelta(days=overlap_days)
        
        # If we've reached the start, break
        if chunk_start <= start:
            break
    
    return chunks


def format_datetime_for_api(date_str: str) -> str:
    """
    Format date string for API (YYYY-MM-DD format).
    
    Args:
        date_str: Date string in YYYY-MM-DD format
    
    Returns:
        str: Formatted date string
    """
    try:
        # Validate and format
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")



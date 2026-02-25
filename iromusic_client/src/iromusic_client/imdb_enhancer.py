"""
IMDb/TMDB Data Enhancement Script

This script enhances the iromusic movie database by fetching additional 
movie information from TMDB (The Movie Database) API.

Features:
- Rate limiting to comply with API requirements
- Database operations (insert/update)
- Comprehensive error handling
- Logging
- Movie details fetching (cast, crew, plot, etc.)

Usage:
    python imdb_enhancer.py --api-key YOUR_TMDB_API_KEY
    python imdb_enhancer.py --search-by-title "Movie Title"
    python imdb_enhancer.py --update-all --limit 10
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('imdb_enhancer.log')
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for API requests"""
    
    def __init__(self, requests_per_second: float = 4.0):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request = 0
    
    def wait(self) -> None:
        """Wait if necessary to comply with rate limit"""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()


class TMDBClient:
    """
    TMDB (The Movie Database) API Client
    
    Fetches movie details, cast, crew, and additional information.
    """
    
    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE_URL = "https://image.tmdb.org/t/p"
    
    def __init__(self, api_key: str, rate_limiter: Optional[RateLimiter] = None):
        self.api_key = api_key
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = self._create_session()
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup additional logging for API responses"""
        self.api_logger = logging.getLogger('TMDB_API')
        self.api_logger.setLevel(logging.DEBUG)
    
    def _create_session(self) -> requests.Session:
        """Create a session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make an API request with rate limiting and error handling
        """
        self.rate_limiter.wait()
        
        url = f"{self.BASE_URL}/{endpoint}"
        params = params or {}
        params['api_key'] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            self.api_logger.debug(f"GET {url} - Status: {response.status_code}")
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e} - {response.status_code}")
            if response.status_code == 401:
                logger.error("Invalid API key!")
            elif response.status_code == 404:
                logger.warning(f"Resource not found: {endpoint}")
            return None
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection Error: {e}")
            return None
            
        except requests.exceptions.Timeout as e:
            logger.error(f"Request Timeout: {e}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Exception: {e}")
            return None
    
    def search_movie(self, title: str, year: Optional[int] = None) -> Optional[Dict]:
        """Search for a movie by title"""
        params = {'query': title}
        if year:
            params['year'] = year
        
        data = self._make_request('search/movie', params)
        
        if data and data.get('results'):
            return data['results'][0]
        return None
    
    def get_movie_details(self, movie_id: int) -> Optional[Dict]:
        """Get detailed information about a movie"""
        return self._make_request(f'movie/{movie_id}')
    
    def get_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """Get movie cast and crew"""
        return self._make_request(f'movie/{movie_id}/credits')
    
    def get_movie_external_ids(self, movie_id: int) -> Optional[Dict]:
        """Get external IDs (IMDb, etc.)"""
        return self._make_request(f'movie/{movie_id}/external_ids')
    
    def get_movie_full_details(self, movie_id: int) -> Dict:
        """
        Get full movie details including credits and external IDs
        """
        details = self.get_movie_details(movie_id)
        if not details:
            return {}
        
        # Get additional data
        credits = self.get_movie_credits(movie_id)
        external_ids = self.get_movie_external_ids(movie_id)
        
        # Combine all data
        result = {
            **details,
            'credits': credits,
            'external_ids': external_ids,
            'fetched_at': datetime.now().isoformat()
        }
        
        return result


class MovieDatabase:
    """
    SQLite database for storing enhanced movie information
    """
    
    def __init__(self, db_path: str = "movies.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Movies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tmdb_id INTEGER UNIQUE,
                imdb_id TEXT,
                iromusic_id INTEGER,
                title TEXT NOT NULL,
                original_title TEXT,
                overview TEXT,
                release_date TEXT,
                runtime INTEGER,
                vote_average REAL,
                vote_count INTEGER,
                popularity REAL,
                genres TEXT,
                production_countries TEXT,
                spoken_languages TEXT,
                status TEXT,
                tagline TEXT,
                budget INTEGER,
                revenue INTEGER,
                poster_path TEXT,
                backdrop_path TEXT,
                fetched_at TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Cast table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cast (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                cast_id INTEGER,
                name TEXT,
                character TEXT,
                order_index INTEGER,
                profile_path TEXT,
                FOREIGN KEY (movie_id) REFERENCES movies(tmdb_id)
            )
        """)
        
        # Crew table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crew (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                crew_id INTEGER,
                name TEXT,
                job TEXT,
                department TEXT,
                profile_path TEXT,
                FOREIGN KEY (movie_id) REFERENCES movies(tmdb_id)
            )
        """)
        
        # Keywords table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER,
                keyword TEXT,
                FOREIGN KEY (movie_id) REFERENCES movies(tmdb_id)
            )
        """)
        
        # Search history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT,
                results_count INTEGER,
                searched_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")
    
    def movie_exists(self, tmdb_id: int) -> bool:
        """Check if a movie exists in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM movies WHERE tmdb_id = ?", (tmdb_id,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    
    def insert_movie(self, movie_data: Dict) -> int:
        """Insert a new movie into the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO movies (
                    tmdb_id, imdb_id, title, original_title, overview, release_date,
                    runtime, vote_average, vote_count, popularity, genres,
                    production_countries, spoken_languages, status, tagline,
                    budget, revenue, poster_path, backdrop_path, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                movie_data.get('id'),
                movie_data.get('external_ids', {}).get('imdb_id'),
                movie_data.get('title'),
                movie_data.get('original_title'),
                movie_data.get('overview'),
                movie_data.get('release_date'),
                movie_data.get('runtime'),
                movie_data.get('vote_average'),
                movie_data.get('vote_count'),
                movie_data.get('popularity'),
                json.dumps(movie_data.get('genres', [])),
                json.dumps(movie_data.get('production_countries', [])),
                json.dumps(movie_data.get('spoken_languages', [])),
                movie_data.get('status'),
                movie_data.get('tagline'),
                movie_data.get('budget'),
                movie_data.get('revenue'),
                movie_data.get('poster_path'),
                movie_data.get('backdrop_path'),
                movie_data.get('fetched_at')
            ))
            
            movie_db_id = cursor.lastrowid
            
            # Insert cast
            if movie_data.get('credits') and movie_data.get('credits').get('cast'):
                for actor in movie_data['credits']['cast'][:10]:  # Top 10 cast
                    cursor.execute("""
                        INSERT INTO cast (movie_id, cast_id, name, character, order_index, profile_path)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        movie_data['id'],
                        actor.get('id'),
                        actor.get('name'),
                        actor.get('character'),
                        actor.get('order'),
                        actor.get('profile_path')
                    ))
            
            # Insert crew (directors, writers)
            if movie_data.get('credits') and movie_data.get('credits').get('crew'):
                for member in movie_data['credits']['crew']:
                    if member.get('job') in ['Director', 'Writer', 'Producer']:
                        cursor.execute("""
                            INSERT INTO crew (movie_id, crew_id, name, job, department, profile_path)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            movie_data['id'],
                            member.get('id'),
                            member.get('name'),
                            member.get('job'),
                            member.get('department'),
                            member.get('profile_path')
                        ))
            
            conn.commit()
            logger.info(f"Inserted movie: {movie_data.get('title')}")
            return movie_db_id
            
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            return -1
        finally:
            conn.close()
    
    def update_movie(self, tmdb_id: int, movie_data: Dict) -> bool:
        """Update an existing movie in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE movies SET
                    imdb_id = ?,
                    title = ?,
                    original_title = ?,
                    overview = ?,
                    release_date = ?,
                    runtime = ?,
                    vote_average = ?,
                    vote_count = ?,
                    popularity = ?,
                    genres = ?,
                    production_countries = ?,
                    spoken_languages = ?,
                    status = ?,
                    tagline = ?,
                    budget = ?,
                    revenue = ?,
                    poster_path = ?,
                    backdrop_path = ?,
                    fetched_at = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE tmdb_id = ?
            """, (
                movie_data.get('external_ids', {}).get('imdb_id'),
                movie_data.get('title'),
                movie_data.get('original_title'),
                movie_data.get('overview'),
                movie_data.get('release_date'),
                movie_data.get('runtime'),
                movie_data.get('vote_average'),
                movie_data.get('vote_count'),
                movie_data.get('popularity'),
                json.dumps(movie_data.get('genres', [])),
                json.dumps(movie_data.get('production_countries', [])),
                json.dumps(movie_data.get('spoken_languages', [])),
                movie_data.get('status'),
                movie_data.get('tagline'),
                movie_data.get('budget'),
                movie_data.get('revenue'),
                movie_data.get('poster_path'),
                movie_data.get('backdrop_path'),
                movie_data.get('fetched_at'),
                tmdb_id
            ))
            
            conn.commit()
            logger.info(f"Updated movie: {movie_data.get('title')}")
            return True
            
        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            return False
        finally:
            conn.close()
    
    def link_iromusic_movie(self, tmdb_id: int, iromusic_id: int) -> bool:
        """Link a TMDB movie to an iromusic movie"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE movies SET iromusic_id = ? WHERE tmdb_id = ?
            """, (iromusic_id, tmdb_id))
            conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error(f"Error linking movies: {e}")
            return False
        finally:
            conn.close()
    
    def get_all_movies(self) -> List[Dict]:
        """Get all movies from database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM movies")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_movie_by_title(self, title: str) -> Optional[Dict]:
        """Get a movie by title"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM movies WHERE title LIKE ?", (f'%{title}%',))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_movie_cast(self, tmdb_id: int) -> List[Dict]:
        """Get cast for a movie"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM cast WHERE movie_id = ? ORDER BY order_index", (tmdb_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def get_movie_crew(self, tmdb_id: int) -> List[Dict]:
        """Get crew for a movie"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM crew WHERE movie_id = ?", (tmdb_id,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    def log_search(self, query: str, results_count: int) -> None:
        """Log a search query"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO search_history (query, results_count) VALUES (?, ?)
        """, (query, results_count))
        conn.commit()
        conn.close()


class IMDBEnhancer:
    """
    Main class for enhancing iromusic movie data with TMDB information
    """
    
    def __init__(self, api_key: str, db_path: str = "movies.db"):
        self.tmdp_client = TMDBClient(api_key)
        self.database = MovieDatabase(db_path)
        self.stats = {
            'processed': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0
        }
    
    def search_and_enhance_by_title(self, title: str, year: Optional[int] = None) -> Optional[Dict]:
        """
        Search for a movie and add to database
        """
        logger.info(f"Searching for: {title} ({year})")
        
        movie = self.tmdp_client.search_movie(title, year)
        
        if not movie:
            logger.warning(f"Movie not found: {title}")
            return None
        
        return self.enhance_movie(movie['id'])
    
    def enhance_movie(self, tmdb_id: int) -> Optional[Dict]:
        """
        Get full details for a movie and save to database
        """
        logger.info(f"Fetching details for TMDB ID: {tmdb_id}")
        
        movie_details = self.tmdp_client.get_movie_full_details(tmdb_id)
        
        if not movie_details:
            logger.error(f"Failed to fetch details for {tmdb_id}")
            self.stats['errors'] += 1
            return None
        
        # Check if movie exists
        if self.database.movie_exists(tmdb_id):
            self.database.update_movie(tmdb_id, movie_details)
            self.stats['updated'] += 1
            logger.info(f"Updated movie: {movie_details.get('title')}")
        else:
            self.database.insert_movie(movie_details)
            self.stats['inserted'] += 1
            logger.info(f"Inserted movie: {movie_details.get('title')}")
        
        self.stats['processed'] += 1
        return movie_details
    
    def enhance_from_iromusic_data(self, iromusic_data_path: str, limit: int = None) -> None:
        """
        Enhance movies from iromusic JSON data
        """
        logger.info(f"Loading iromusic data from: {iromusic_data_path}")
        
        with open(iromusic_data_path, 'r', encoding='utf-8') as f:
            movies = json.load(f)
        
        if limit:
            movies = movies[:limit]
        
        logger.info(f"Processing {len(movies)} movies...")
        
        for movie in movies:
            try:
                title = movie.get('englishTitle', '')
                year = movie.get('year')
                
                if year:
                    try:
                        year = int(year)
                    except (ValueError, TypeError):
                        year = None
                
                # Search TMDB
                result = self.search_and_enhance_by_title(title, year)
                
                if result:
                    # Link to iromusic movie
                    self.database.link_iromusic_movie(
                        result['id'],
                        movie.get('id')
                    )
                
            except Exception as e:
                logger.error(f"Error processing movie: {e}")
                self.stats['errors'] += 1
    
    def get_stats(self) -> Dict:
        """Get processing statistics"""
        return self.stats


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Enhance iromusic movie data with TMDB/IMDb information'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        default=os.environ.get('TMDB_API_KEY', ''),
        help='TMDB API key (or set TMDB_API_KEY environment variable)'
    )
    
    parser.add_argument(
        '--search',
        type=str,
        help='Search for a movie by title'
    )
    
    parser.add_argument(
        '--year',
        type=int,
        help='Release year for search'
    )
    
    parser.add_argument(
        '--update-all',
        action='store_true',
        help='Update all movies from iromusic data'
    )
    
    parser.add_argument(
        '--iromusic-path',
        type=str,
        default='iromusic_client/output/2026/02/20/movie',
        help='Path to iromusic movie data'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Limit number of movies to process'
    )
    
    parser.add_argument(
        '--db-path',
        type=str,
        default='movies.db',
        help='Path to SQLite database'
    )
    
    args = parser.parse_args()
    
    if not args.api_key:
        parser.error("API key required. Get one free at https://www.themoviedb.org/settings/api")
    
    # Create enhancer
    enhancer = IMDBEnhancer(args.api_key, args.db_path)
    
    if args.search:
        # Single search mode
        result = enhancer.search_and_enhance_by_title(args.search, args.year)
        if result:
            print(f"\n✓ Movie found and saved to database!")
            print(f"  Title: {result.get('title')}")
            print(f"  Release Date: {result.get('release_date')}")
            print(f"  Rating: {result.get('vote_average')}/10")
            
            # Show cast
            cast = enhancer.database.get_movie_cast(result['id'])
            if cast:
                print(f"  Cast: {', '.join([a['name'] for a in cast[:5]])}")
    else:
        # Batch mode - process iromusic data
        # Find latest iromusic file
        iromusic_path = Path(args.iromusic_path)
        if iromusic_path.exists():
            json_files = list(iromusic_path.glob("*.json"))
            if json_files:
                latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
                print(f"Processing: {latest_file}")
                enhancer.enhance_from_iromusic_data(str(latest_file), args.limit)
                
                stats = enhancer.get_stats()
                print(f"\n{'='*50}")
                print("PROCESSING COMPLETE")
                print(f"{'='*50}")
                print(f"Processed: {stats['processed']}")
                print(f"Inserted: {stats['inserted']}")
                print(f"Updated: {stats['updated']}")
                print(f"Errors: {stats['errors']}")
        else:
            logger.error(f"Path not found: {args.iromusic_path}")


if __name__ == "__main__":
    main()

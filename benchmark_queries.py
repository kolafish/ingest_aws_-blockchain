#!/usr/bin/env python3
import os
import sys
import time
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine


# Configuration
DEFAULT_DB_CONFIG = {
    "user": os.getenv("TIDB_USER", "root"),
    "password": os.getenv("TIDB_PASSWORD", ""),
    "host": os.getenv("TIDB_HOST", "127.0.0.1"),
    "port": int(os.getenv("TIDB_PORT", "4001")),
    "database": os.getenv("TIDB_DB", "test")
}

# Query patterns configuration
DEFAULT_QUERIES_PER_SECOND = 5
DEFAULT_DURATION_SECONDS = 120
DEFAULT_MAX_CONCURRENT_QUERIES = 10


def get_engine(db_config: Dict[str, Any] = None) -> Engine:
    """Create database engine"""
    if db_config is None:
        db_config = DEFAULT_DB_CONFIG

    url = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


class QuerySampler:
    """Sample real values from database for realistic queries"""

    def __init__(self, engine: Engine, table_name: str):
        self.engine = engine
        self.table_name = table_name
        self.sampled_data = {}
        self.range_data = {}

    def sample_values(self, column: str, sample_size: int = 1000, silent: bool = False) -> List[Any]:
        """Sample distinct values from a column"""
        if column in self.sampled_data:
            return self.sampled_data[column]

        try:
            # For hash column, sample more values since hashes should be unique
            if column == 'hash':
                sample_size = 5000  # Sample more hashes to get variety
            sql_query = f"SELECT DISTINCT {column} FROM test.{self.table_name} WHERE {column} IS NOT NULL LIMIT {sample_size}"
            if not silent:
                print(f"[SAMPLING_SQL] {sql_query}")
            query = text(sql_query)
            with self.engine.connect() as conn:
                result = conn.execute(query)
                values = [row[0] for row in result.fetchall()]
                # Convert block_timestamp from DATETIME to Unix timestamp for query generation
                if column == 'block_timestamp':
                    converted_values = []
                    for v in values:
                        if v is None:
                            converted_values.append(None)
                        elif isinstance(v, datetime):
                            # Already a datetime object from database
                            converted_values.append(int(v.timestamp()))
                        elif isinstance(v, (int, float)):
                            # Already a Unix timestamp
                            converted_values.append(int(v))
                        else:
                            # Try to parse as datetime string
                            try:
                                dt = datetime.strptime(str(v), '%Y-%m-%d %H:%M:%S')
                                converted_values.append(int(dt.timestamp()))
                            except:
                                converted_values.append(v)
                    values = converted_values
                self.sampled_data[column] = values
                if not silent and values:
                    print(f"[SAMPLING] Column {column}: sampled {len(values)} distinct values")
                elif not silent and not values:
                    print(f"[SAMPLING] Column {column}: no values found!")
                return values
        except Exception as e:
            logging.warning(f"Failed to sample {column}: {e}")
            return []

    def get_random_value(self, column: str) -> Any:
        """Get a random value from sampled data"""
        values = self.sample_values(column)
        value = random.choice(values) if values else None
        # Ensure block_timestamp is returned as integer
        if column == 'block_timestamp' and value is not None:
            return int(value)
        return value

    def get_value_range(self, column: str, silent: bool = False) -> Tuple[Any, Any]:
        """Get min/max range for a column"""
        if column in self.range_data:
            return self.range_data[column]

        try:
            sql_query = f"SELECT MIN({column}), MAX({column}) FROM test.{self.table_name} WHERE {column} IS NOT NULL"
            if not silent:
                print(f"[SAMPLING_SQL] {sql_query}")
            query = text(sql_query)
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                min_val, max_val = result[0], result[1]
                # Convert block_timestamp from DATETIME to Unix timestamp for query generation
                if column == 'block_timestamp':
                    if min_val is not None:
                        if isinstance(min_val, datetime):
                            min_val = int(min_val.timestamp())
                        elif isinstance(min_val, (int, float)):
                            min_val = int(min_val)
                        else:
                            try:
                                dt = datetime.strptime(str(min_val), '%Y-%m-%d %H:%M:%S')
                                min_val = int(dt.timestamp())
                            except:
                                min_val = None
                    if max_val is not None:
                        if isinstance(max_val, datetime):
                            max_val = int(max_val.timestamp())
                        elif isinstance(max_val, (int, float)):
                            max_val = int(max_val)
                        else:
                            try:
                                dt = datetime.strptime(str(max_val), '%Y-%m-%d %H:%M:%S')
                                max_val = int(dt.timestamp())
                            except:
                                max_val = None
                range_tuple = (min_val, max_val)
                self.range_data[column] = range_tuple
                return range_tuple
        except Exception as e:
            logging.warning(f"Failed to get range for {column}: {e}")
            return None, None


class QueryGenerator:
    """Generate diverse queries for benchmarking"""

    # Column categories for different query types
    STRING_COLUMNS = ['date', 'hash', 'from_address', 'to_address', 'receipt_contract_address']
    NUMERIC_COLUMNS = ['block_timestamp', 'block_number', 'transaction_index', 'gas', 'gas_price', 'receipt_status', 'transaction_type']
    BOOLEAN_COLUMNS = ['random_flag']

    # Column data types (from table schema) - will be adjusted based on actual table schema
    DATETIME_COLUMNS = []  # Will be populated based on actual schema
    INTEGER_COLUMNS = ['block_number', 'transaction_index', 'gas', 'gas_price', 'receipt_status', 'transaction_type', 'nonce']
    FLOAT_COLUMNS = ['value']  # Only value is DOUBLE in the schema

    # Sort columns from hybrid index
    SORT_COLUMNS = ['block_timestamp', 'gas_price']

    def __init__(self, sampler: QuerySampler):
        self.sampler = sampler
        # Detect block_timestamp data type from database schema
        self._detect_block_timestamp_type()

    def _detect_block_timestamp_type(self):
        """Detect block_timestamp column data type from database schema"""
        try:
            # Query information_schema to get column data type
            sql = f"""
                SELECT DATA_TYPE 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = 'test' 
                AND TABLE_NAME = :table_name 
                AND COLUMN_NAME = 'block_timestamp'
            """
            query = text(sql)
            with self.sampler.engine.connect() as conn:
                result = conn.execute(query, {"table_name": self.sampler.table_name})
                row = result.fetchone()
                if row:
                    data_type = row[0].upper()
                    if data_type in ['DATETIME', 'TIMESTAMP']:
                        # block_timestamp is DATETIME type
                        self.DATETIME_COLUMNS = ['block_timestamp']
                        logging.info(f"Detected block_timestamp as DATETIME type")
                    elif data_type in ['BIGINT', 'INT', 'INTEGER']:
                        # block_timestamp is BIGINT type
                        self.DATETIME_COLUMNS = []
                        # Add block_timestamp to INTEGER_COLUMNS if not already there
                        if 'block_timestamp' not in self.INTEGER_COLUMNS:
                            self.INTEGER_COLUMNS.append('block_timestamp')
                        logging.info(f"Detected block_timestamp as {data_type} type")
                    else:
                        # Default to DATETIME if unknown
                        self.DATETIME_COLUMNS = ['block_timestamp']
                        logging.warning(f"Unknown block_timestamp type: {data_type}, defaulting to DATETIME")
                else:
                    # Column not found, default to DATETIME
                    self.DATETIME_COLUMNS = ['block_timestamp']
                    logging.warning("Could not detect block_timestamp type, defaulting to DATETIME")
        except Exception as e:
            # On error, default to DATETIME
            self.DATETIME_COLUMNS = ['block_timestamp']
            logging.warning(f"Error detecting block_timestamp type: {e}, defaulting to DATETIME")

    def format_numeric_value(self, value: float, column: str) -> str:
        """Format numeric value according to column data type"""
        if column in self.DATETIME_COLUMNS:
            # Convert Unix timestamp to DATETIME format for block_timestamp
            try:
                dt = datetime.fromtimestamp(value)
                return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
            except (ValueError, OSError, OverflowError):
                return str(value)
        elif column in self.INTEGER_COLUMNS:
            # Convert to integer for integer columns
            return str(int(value))
        elif column in self.FLOAT_COLUMNS:
            # Keep as float for double columns
            return f"{value:.6f}".rstrip('0').rstrip('.')
        else:
            # Default to float formatting
            return str(value)

    def generate_random_conditions(self, num_conditions: int = None) -> List[str]:
        """Generate random WHERE conditions"""
        if num_conditions is None:
            num_conditions = random.randint(1, 4)  # 1-4 conditions

        conditions = []
        used_columns = set()

        for _ in range(num_conditions):
            # Choose query type with preference for ranges (more likely to match)
            query_type = random.choice(['string_eq', 'numeric_range', 'numeric_range', 'numeric_eq', 'boolean_eq'])

            if query_type == 'string_eq':
                # String equality (good for inverted index)
                available_cols = [c for c in self.STRING_COLUMNS if c not in used_columns]
                if not available_cols:
                    continue
                column = random.choice(available_cols)
                value = self.sampler.get_random_value(column)
                if value is not None:
                    conditions.append(f"{column} = '{value}'")
                    used_columns.add(column)

            elif query_type == 'numeric_range':
                # Numeric range (good for inverted index)
                available_cols = [c for c in self.NUMERIC_COLUMNS if c not in used_columns]
                if not available_cols:
                    continue
                column = random.choice(available_cols)
                min_val, max_val = self.sampler.get_value_range(column)
                if min_val is not None and max_val is not None:
                    # Generate random range within the actual data range
                    range_size = max_val - min_val
                    if range_size > 0:
                        # Use wider ranges for better matching (10%-50% of total range)
                        start_offset = random.uniform(0, 0.5)
                        range_width = random.uniform(0.1, 0.5)

                        start_val = min_val + (range_size * start_offset)
                        end_val = start_val + (range_size * range_width)

                        # Ensure end_val doesn't exceed max_val
                        end_val = min(end_val, max_val)

                        # Format values according to column data type
                        start_str = self.format_numeric_value(start_val, column)
                        end_str = self.format_numeric_value(end_val, column)
                        
                        # Check if range is too small (start_val == end_val or formatted values are equal)
                        # If so, use equality instead of BETWEEN
                        if abs(end_val - start_val) < 1e-6 or start_str == end_str:
                            # Range is effectively zero, use equality
                            conditions.append(f"{column} = {start_str}")
                        else:
                            conditions.append(f"{column} BETWEEN {start_str} AND {end_str}")
                        used_columns.add(column)

            elif query_type == 'numeric_eq':
                # Numeric equality
                available_cols = [c for c in self.NUMERIC_COLUMNS if c not in used_columns]
                if not available_cols:
                    continue
                column = random.choice(available_cols)
                value = self.sampler.get_random_value(column)
                if value is not None:
                    # Format value according to column data type
                    value_str = self.format_numeric_value(value, column)
                    conditions.append(f"{column} = {value_str}")
                    used_columns.add(column)

            elif query_type == 'boolean_eq':
                # Boolean equality
                available_cols = [c for c in self.BOOLEAN_COLUMNS if c not in used_columns]
                if not available_cols:
                    continue
                column = random.choice(available_cols)
                value = self.sampler.get_random_value(column)
                if value is not None:
                    # Convert boolean to SQL boolean (True/False or 1/0)
                    bool_value = 'TRUE' if value else 'FALSE'
                    conditions.append(f"{column} = {bool_value}")
                    used_columns.add(column)

        return conditions

    def generate_simple_conditions(self) -> List[str]:
        """Generate simple conditions with just one condition to ensure results"""
        conditions = []

        # Try to get a simple condition that should return results
        # Prefer string equality as it's more likely to match
        rand_val = random.random()
        if self.STRING_COLUMNS and rand_val < 0.5:
            # Try string columns first
            random.shuffle(self.STRING_COLUMNS)  # Shuffle to try different columns
            for col in self.STRING_COLUMNS:
                value = self.sampler.get_random_value(col)
                if value is not None:
                    conditions.append(f"{col} = '{value}'")
                    break
        elif self.BOOLEAN_COLUMNS and rand_val < 0.7:
            # Try boolean columns
            random.shuffle(self.BOOLEAN_COLUMNS)
            for col in self.BOOLEAN_COLUMNS:
                value = self.sampler.get_random_value(col)
                if value is not None:
                    bool_value = 'TRUE' if value else 'FALSE'
                    conditions.append(f"{col} = {bool_value}")
                    break
        else:
            # Fall back to numeric range
            if self.NUMERIC_COLUMNS:
                random.shuffle(self.NUMERIC_COLUMNS)
                for col in self.NUMERIC_COLUMNS:
                    min_val, max_val = self.sampler.get_value_range(col)
                    if min_val is not None and max_val is not None:
                        # Use a wider range (50% of total range) to ensure results
                        range_size = max_val - min_val
                        if range_size > 0:
                            # Start from a random position and take 50% of the range
                            start_offset = random.uniform(0, 0.5)
                            start_val = min_val + (range_size * start_offset)
                            end_val = start_val + (range_size * 0.5)
                            end_val = min(end_val, max_val)

                            # Format values according to column data type
                            start_str = self.format_numeric_value(start_val, col)
                            end_str = self.format_numeric_value(end_val, col)
                            
                            # Check if range is too small (start_val == end_val or formatted values are equal)
                            # If so, use equality instead of BETWEEN
                            if abs(end_val - start_val) < 1e-6 or start_str == end_str:
                                # Range is effectively zero, use equality
                                conditions.append(f"{col} = {start_str}")
                            else:
                                conditions.append(f"{col} BETWEEN {start_str} AND {end_str}")
                            break

        return conditions

    def generate_order_by(self, use_order: bool = None, force_order: bool = False) -> str:
        """Generate ORDER BY clause using sort index columns"""
        if force_order:
            use_order = True
        elif use_order is None:
            use_order = random.choice([True, False, False])  # 33% chance of ordering

        if not use_order:
            return ""

        # Always use descending for sort columns as per index
        order_parts = []
        for col in self.SORT_COLUMNS:
            order_parts.append(f"{col} DESC")

        return f"ORDER BY {', '.join(order_parts)}"

    def generate_limit(self, has_order: bool) -> str:
        """Generate LIMIT clause"""
        if has_order:
            # For ordered queries, small limit to test sort performance
            return f"LIMIT {random.randint(10, 200)}"
        else:
            # For non-ordered queries, larger limit or no limit
            return "" if random.random() < 0.7 else f"LIMIT {random.randint(1000, 10000)}"

    def generate_query(self) -> Tuple[str, str]:
        """Generate a complete query with metadata"""
        # Choose query type (with higher probability for simpler queries)
        query_weights = [
            ('select_cols', 40),    # SELECT specific columns (most common)
            ('select_star', 40),    # SELECT * (most common)
            ('simple_select', 20),  # Simple query with one condition (less strict)
        ]
        query_types = [qt for qt, weight in query_weights for _ in range(weight)]
        query_type = random.choice(query_types)

        # Generate WHERE conditions
        if query_type == 'simple_select':
            # Simple query with just one condition
            conditions = self.generate_simple_conditions()
        else:
            conditions = self.generate_random_conditions()
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Analyze conditions to determine if ORDER BY should be forced
        # Force ORDER BY if query might return many results (has range conditions without equality filters)
        force_order = False
        if conditions:
            has_range_conditions = any('BETWEEN' in cond for cond in conditions)
            has_equality_conditions = any('=' in cond and 'BETWEEN' not in cond for cond in conditions)
            # Force ORDER BY if there are range conditions but no equality conditions (likely to return many rows)
            force_order = has_range_conditions and not has_equality_conditions

        # Generate ORDER BY and LIMIT
        order_by = self.generate_order_by(force_order=force_order)
        has_order = bool(order_by)
        limit_clause = self.generate_limit(has_order)

        # Build the query
        if query_type == 'select_cols':
            # Select more columns (5-10 instead of 3-7)
            available_cols = ['hash', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'block_timestamp', 'block_number', 'nonce', 'input', 'receipt_gas_used', 'random_flag']
            num_cols = min(len(available_cols), random.randint(5, 10))
            select_cols = random.sample(available_cols, k=num_cols)
            cols_str = ", ".join(select_cols)
            sql = f"SELECT {cols_str} FROM test.{self.sampler.table_name} WHERE {where_clause}"
            query_type_desc = f"SELECT_COLS({len(select_cols)})"
        elif query_type == 'simple_select':
            # Simple select with fewer columns for better performance
            available_cols = ['hash', 'from_address', 'to_address', 'value', 'gas_price', 'block_timestamp', 'random_flag']
            num_cols = min(len(available_cols), random.randint(3, 6))
            select_cols = random.sample(available_cols, k=num_cols)
            cols_str = ", ".join(select_cols)
            sql = f"SELECT {cols_str} FROM test.{self.sampler.table_name} WHERE {where_clause}"
            query_type_desc = f"SIMPLE_SELECT({len(select_cols)})"
        else:  # select_star
            sql = f"SELECT * FROM test.{self.sampler.table_name} WHERE {where_clause}"
            query_type_desc = "SELECT_STAR"

        # Add ORDER BY and LIMIT if applicable
        if order_by:
            sql += f" {order_by}"
        if limit_clause:
            sql += f" {limit_clause}"

        # Create description for logging
        desc_parts = [query_type_desc]
        if conditions:
            desc_parts.append(f"conditions({len(conditions)})")
        if has_order:
            desc_parts.append("ordered")
        if limit_clause:
            desc_parts.append("limited")

        description = "_".join(desc_parts)
        return sql, description


def execute_query(engine: Engine, sql: str) -> Tuple[int, float, Optional[int]]:
    """Execute a query and return row count and timing"""
    start_time = time.perf_counter()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            # For SELECT queries, fetch all results to measure full query time
            if sql.strip().upper().startswith('SELECT'):
                rows = result.fetchall()
                row_count = len(rows)
            else:
                row_count = result.scalar() or 0
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return 0, 0.0, None

    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return row_count, elapsed_ms, None


def benchmark_queries(table_name: str, queries_per_second: int = DEFAULT_QUERIES_PER_SECOND,
                     duration_seconds: int = DEFAULT_DURATION_SECONDS, max_concurrent: int = DEFAULT_MAX_CONCURRENT_QUERIES,
                     verbose_sql: bool = True):
    """Run query benchmarking"""
    configure_logging()
    logging.info(f"Starting benchmark: {queries_per_second} qps for {duration_seconds}s, table={table_name}")

    engine = get_engine()
    sampler = QuerySampler(engine, table_name)
    generator = QueryGenerator(sampler)

    # Warm up the sampler (silent mode - no SQL output during sampling)
    logging.info("Warming up sampler...")
    print("[SAMPLING] Starting data sampling for all columns...")
    for col in QueryGenerator.STRING_COLUMNS + QueryGenerator.NUMERIC_COLUMNS + QueryGenerator.BOOLEAN_COLUMNS:
        print(f"[SAMPLING] Sampling values for column: {col}")
        values = sampler.sample_values(col, 100, silent=True)
        print(f"[SAMPLING] Column {col}: sampled {len(values)} distinct values")
        # Also preload range data for numeric columns
        if col in QueryGenerator.NUMERIC_COLUMNS:
            min_val, max_val = sampler.get_value_range(col, silent=True)
            if min_val is not None and max_val is not None:
                print(f"[SAMPLING] Column {col}: range [{min_val}, {max_val}]")
            else:
                print(f"[SAMPLING] Column {col}: no range data found!")
    print("[SAMPLING] Data sampling completed")
    print()

    start_time = time.time()
    end_time = start_time + duration_seconds

    stats = {
        'total_queries': 0,
        'total_time_ms': 0.0,
        'total_rows': 0,
        'query_types': {},
        'errors': 0
    }

    query_count = 0
    while time.time() < end_time:
        query_start = time.time()

        # Generate and execute query
        try:
            sql, query_desc = generator.generate_query()
            logging.debug(f"Executing query: {sql}")
            if verbose_sql:
                print(f"[SQL] {sql}")
            row_count, elapsed_ms, _ = execute_query(engine, sql)

            stats['total_queries'] += 1
            stats['total_time_ms'] += elapsed_ms
            stats['total_rows'] += row_count

            # Print execution results
            print(f"[RESULT] Rows: {row_count}, Time: {elapsed_ms:.1f}ms, Type: {query_desc}")
            if verbose_sql:
                print()

            # Track query type stats
            if query_desc not in stats['query_types']:
                stats['query_types'][query_desc] = {'count': 0, 'total_ms': 0.0, 'total_rows': 0}
            stats['query_types'][query_desc]['count'] += 1
            stats['query_types'][query_desc]['total_ms'] += elapsed_ms
            stats['query_types'][query_desc]['total_rows'] += row_count

            # Log progress periodically
            query_count += 1
            if query_count % 50 == 0:
                avg_ms = stats['total_time_ms'] / stats['total_queries']
                logging.info(f"Progress: {query_count} queries, avg {avg_ms:.1f}ms, total_rows {stats['total_rows']}")

        except Exception as e:
            stats['errors'] += 1
            logging.error(f"Query generation/execution failed: {e}")
            print(f"[ERROR] Query failed: {e}")
            # Print the SQL that failed for debugging
            if 'sql' in locals():
                print(f"[ERROR] Failed SQL: {sql}")
            print()

        # Throttle to target QPS
        elapsed = time.time() - query_start
        sleep_time = max(0, (1.0 / queries_per_second) - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Print final statistics
    total_time_sec = time.time() - start_time
    actual_qps = stats['total_queries'] / total_time_sec

    print("\n" + "="*80)
    print(f"BENCHMARK RESULTS ({table_name})")
    print(f"Duration: {total_time_sec:.1f}s")
    print(f"Total queries: {stats['total_queries']}")
    print(f"Actual QPS: {actual_qps:.1f}")
    print(f"Average latency: {stats['total_time_ms']/stats['total_queries']:.1f}ms")
    print(f"Total rows returned: {stats['total_rows']}")
    print(f"Errors: {stats['errors']}")
    print("\nQuery Type Breakdown:")

    for qtype, qstats in sorted(stats['query_types'].items(), key=lambda x: x[1]['count'], reverse=True):
        avg_ms = qstats['total_ms'] / qstats['count']
        print(f"  {qtype}: {qstats['count']} queries, {avg_ms:.1f}ms avg, {qstats['total_rows']} rows")

    print("="*80)


def configure_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Query benchmarking for Ethereum transactions")
    parser.add_argument("--table", required=True, help="Table name to benchmark")
    parser.add_argument("--qps", type=int, default=DEFAULT_QUERIES_PER_SECOND, help="Queries per second")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SECONDS, help="Duration in seconds")
    parser.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT_QUERIES, help="Max concurrent queries")
    parser.add_argument("--verbose-sql", action="store_true", default=True, help="Show detailed SQL queries")
    parser.add_argument("--quiet-sql", action="store_true", help="Hide SQL queries, only show results")

    args = parser.parse_args()
    verbose_sql = args.verbose_sql and not args.quiet_sql
    benchmark_queries(args.table, args.qps, args.duration, args.max_concurrent, verbose_sql)


if __name__ == "__main__":
    main()

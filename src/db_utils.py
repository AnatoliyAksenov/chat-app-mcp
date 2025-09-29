from sqlalchemy import inspect, MetaData, Table
from sqlalchemy import select, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import CreateTable

def get_table_ddl(engine, table_name):
    _schema, _table_name = table_name.split('.') if table_name.count('.') == 1 else [table_name, 'public']
    
    try:
        inspector = inspect(engine)
        
        if not inspector.has_table(_table_name, _schema):
            print(f"Table {table_name} does not exist")
            return None
        
        metadata = MetaData()
        table = Table(_table_name, metadata, autoload_with=engine, schema=_schema)
        
        # Get table comment
        table_comment = inspector.get_table_comment(_table_name, schema=_schema)
        
        # Generate DDL
        from sqlalchemy.schema import CreateTable
        ddl = str(CreateTable(table).compile(engine))
        
        # Generate table commet
        comment_text = table_comment.get('text')
        if comment_text:
            # Remove trailing semicolon from base DDL
            ddl = ddl.rstrip().rstrip(';')
            
            # Add comment based on database dialect
            dialect_name = engine.dialect.name.lower()
            
            if dialect_name == 'postgresql':
                ddl += f"\nCOMMENT ON TABLE {table_name} IS '{comment_text}';"
            elif dialect_name in ['mysql', 'mariadb']:
                ddl += f" COMMENT='{comment_text}';"
            elif dialect_name == 'sqlite':
                # SQLite doesn't support table comments in standard DDL
                ddl += ';'
                ddl += f"\n-- Table comment: {comment_text}"
            else:
                ddl += f";\n-- Table comment: {comment_text}"
        else:
            ddl = ddl.rstrip()  # Ensure proper formatting
        
        
        # Add column comments
        columns = inspector.get_columns(_table_name, schema=_schema)
        for col in columns:
            if col.get('comment'):
                ddl += f"\nCOMMENT ON COLUMN {_table_name}.{col['name']} IS '{col['comment']}';"
        
        return ddl
        
    except NoSuchTableError:
        print(f"Table {table_name} not found")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    

def get_table_sample(engine, table, sample_size=100, schema=None) -> list[dict]:
    """Sample from non-partitioned table"""
    from sqlalchemy import Table, MetaData
    
    metadata = MetaData()
    table_obj = Table(table, metadata, autoload_with=engine, schema=schema)
    
    # Use database-specific sampling if available
    dialect_name = engine.dialect.name.lower()
    
    if dialect_name == 'postgresql':
        # PostgreSQL TABLESAMPLE
        query = select(table_obj).order_by(func.random()).limit(sample_size)
    elif dialect_name in ['mysql', 'mariadb']:
        # MySQL RAND()
        query = select(table_obj).order_by(func.rand()).limit(sample_size)
    else:
        # Generic random sampling
        query = select(table_obj).order_by(func.random()).limit(sample_size)

    Session = sessionmaker(bind=engine)
    with Session() as s:
        result = s.execute(query).fetchall()
        s.commit()

    return [dict(row._mapping) for row in result]


def get_table_ddl_with_partitions(engine, table_name, schema=None):
    """
    Generate complete DDL including partition definitions, comments, and constraints
    """
    try:
        inspector = inspect(engine)
        dialect_name = engine.dialect.name.lower()
        
        if not inspector.has_table(table_name, schema):
            print(f"Table '{table_name}' does not exist")
            return None
        
        # Reflect the table
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        
        # Generate base DDL
        ddl_parts = [str(CreateTable(table).compile(engine)).rstrip().rstrip(';')]
        
        # Add partition definition
        partition_ddl = get_partition_ddl(engine, table_name, schema)
        if partition_ddl:
            ddl_parts[0] += f"\n{partition_ddl}"
        
        # Add table comment
        table_comment = inspector.get_table_comment(table_name, schema=schema)
        if table_comment.get('text'):
            if dialect_name == 'postgresql':
                ddl_parts.append(f"COMMENT ON TABLE {table_name} IS '{table_comment['text']}';")
            elif dialect_name in ['mysql', 'mariadb']:
                ddl_parts[0] = ddl_parts[0].rstrip(';') + f" COMMENT='{table_comment['text']}'"
        
        # Add column comments
        columns = inspector.get_columns(table_name, schema=schema)
        for col in columns:
            if col.get('comment'):
                if dialect_name == 'postgresql':
                    ddl_parts.append(f"COMMENT ON COLUMN {table_name}.{col['name']} IS '{col['comment']}';")
        
        # Add indexes and constraints
        indexes_ddl = get_indexes_ddl(engine, table_name, schema)
        if indexes_ddl:
            ddl_parts.extend(indexes_ddl)
        
        return '\n'.join(ddl_parts)
        
    except NoSuchTableError:
        print(f"Table '{table_name}' not found")
        return None
    except Exception as e:
        print(f"Error generating DDL: {e}")
        return None


def get_partition_ddl(engine, table_name, schema=None):
    """
    Get partition definition DDL for the table
    """
    dialect_name = engine.dialect.name.lower()
    
    if dialect_name == 'postgresql':
        return get_postgresql_partition_ddl(engine, table_name, schema)
    elif dialect_name in ['mysql', 'mariadb']:
        return get_mysql_partition_ddl(engine, table_name, schema)
    elif dialect_name == 'oracle':
        return get_oracle_partition_ddl(engine, table_name, schema)
    else:
        return None


def get_postgresql_partition_ddl(engine, table_name, schema=None):
    """Get PostgreSQL partition DDL"""
    try:
        # Check if table is partitioned
        query = text("""
            SELECT partstrat, pa.attname as partition_key_col
            FROM pg_partitioned_table t
            JOIN pg_class ON t.partrelid = pg_class.oid
            JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
            LEFT JOIN pg_attribute AS pa ON pa.attrelid = pg_class.oid AND pa.attnum = ANY(t.partattrs)
            WHERE pg_class.relname = :table_name
            AND pg_namespace.nspname = COALESCE(:schema, 'public')
        """)

        # Get partition key definition
        key_query = text("""
            SELECT pg_get_expr(partexprs, partrelid) as partition_expr
            FROM pg_partitioned_table
            JOIN pg_class ON pg_partitioned_table.partrelid = pg_class.oid
            WHERE pg_class.relname = :table_name
        """)

        Session = sessionmaker(bind=engine)
        with Session() as s:
            result = s.execute(query,{'table_name': table_name, 'schema': schema}).fetchall()
            key_result = s.execute(key_query, {'table_name': table_name, }).fetchall()
            s.commit()

        if not result and not key_result:
            return None
        
        partition_strategy = result[0][0]  # 'h'=hash, 'r'=range, 'l'=list
        partition_key = result[0][1]
        
        partition_expr = key_result[0][0] if len(key_result) == 1 else None
        
        # Map strategy codes to SQL keywords
        strategy_map = {'h': 'HASH', 'r': 'RANGE', 'l': 'LIST'}
        strategy = strategy_map.get(partition_strategy, 'RANGE')
        
        if partition_expr:
            return f"PARTITION BY {strategy} ({partition_expr})"
        elif result:
            columns = ', '.join([row[1] for row in result])
            return f"PARTITION BY {strategy} ({columns})"
        
        return None
        
    except Exception as e:
        print(f"Error getting PostgreSQL partition info: {e}")
        return None


def get_mysql_partition_ddl(engine, table_name, schema=None):
    """Get MySQL partition DDL"""
    try:
        query = text("""
            SELECT PARTITION_METHOD, PARTITION_EXPRESSION, SUBPARTITION_METHOD, SUBPARTITION_EXPRESSION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_NAME = :table_name
            AND TABLE_SCHEMA = COALESCE(:schema, DATABASE())
            AND PARTITION_NAME IS NULL
            LIMIT 1
        """)
        
        result = engine.execute(query, {
            'table_name': table_name,
            'schema': schema
        }).fetchone()
        
        if not result or not result[0]:  # No partition method
            return None
        
        partition_method = result[0]
        partition_expression = result[1]
        subpartition_method = result[2]
        subpartition_expression = result[3]
        
        ddl = f"PARTITION BY {partition_method.upper()}({partition_expression})"
        
        if subpartition_method:
            ddl += f"\nSUBPARTITION BY {subpartition_method.upper()}({subpartition_expression})"
        
        # Get partition definitions
        part_query = text("""
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION, PARTITION_COMMENT
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_NAME = :table_name
            AND TABLE_SCHEMA = COALESCE(:schema, DATABASE())
            AND PARTITION_NAME IS NOT NULL
            GROUP BY PARTITION_NAME, PARTITION_DESCRIPTION, PARTITION_COMMENT
            ORDER BY PARTITION_ORDINAL_POSITION
        """)
        
        partitions = engine.execute(part_query, {
            'table_name': table_name,
            'schema': schema
        }).fetchall()
        
        if partitions:
            ddl += " (\n"
            partition_defs = []
            
            for part_name, part_desc, part_comment in partitions:
                part_def = f"  PARTITION {part_name}"
                
                if partition_method.upper() in ['RANGE', 'LIST']:
                    if part_desc:
                        part_def += f" VALUES {partition_method.upper()} ({part_desc})"
                
                if part_comment:
                    part_def += f" COMMENT = '{part_comment}'"
                
                partition_defs.append(part_def)
            
            ddl += ',\n'.join(partition_defs)
            ddl += "\n)"
        
        return ddl
        
    except Exception as e:
        print(f"Error getting MySQL partition info: {e}")
        return None

def get_oracle_partition_ddl(engine, table_name, schema=None):
    """Get Oracle partition DDL"""
    try:
        query = text("""
            SELECT partitioning_type, subpartitioning_type, partition_count
            FROM all_part_tables
            WHERE table_name = :table_name
            AND owner = COALESCE(:schema, USER)
        """)
        
        result = engine.execute(query, {
            'table_name': table_name.upper(),
            'schema': schema.upper() if schema else None
        }).fetchone()
        
        if not result:
            return None
        
        partition_type = result[0]
        subpartition_type = result[1]
        
        # Get partition key columns
        key_query = text("""
            SELECT column_name, column_position
            FROM all_part_key_columns
            WHERE name = :table_name
            AND owner = COALESCE(:schema, USER)
            ORDER BY column_position
        """)
        
        key_columns = engine.execute(key_query, {
            'table_name': table_name.upper(),
            'schema': schema.upper() if schema else None
        }).fetchall()
        
        if not key_columns:
            return None
        
        columns = ', '.join([row[0] for row in key_columns])
        ddl = f"PARTITION BY {partition_type} ({columns})"
        
        if subpartition_type:
            # Get subpartition key columns
            sub_key_query = text("""
                SELECT column_name, column_position
                FROM all_subpart_key_columns
                WHERE name = :table_name
                AND owner = COALESCE(:schema, USER)
                ORDER BY column_position
            """)
            
            sub_key_columns = engine.execute(sub_key_query, {
                'table_name': table_name.upper(),
                'schema': schema.upper() if schema else None
            }).fetchall()
            
            if sub_key_columns:
                sub_columns = ', '.join([row[0] for row in sub_key_columns])
                ddl += f"\nSUBPARTITION BY {subpartition_type} ({sub_columns})"
        
        return ddl
        
    except Exception as e:
        print(f"Error getting Oracle partition info: {e}")
        return None

def get_indexes_ddl(engine, table_name, schema=None):
    """Get CREATE INDEX statements"""
    try:
        inspector = inspect(engine)
        indexes = inspector.get_indexes(table_name, schema=schema)
        index_ddl = []
        
        for index in indexes:
            if index['name'].startswith('pg_') or index['name'].startswith('sqlite_'):
                continue  # Skip system indexes
                
            unique = 'UNIQUE ' if index.get('unique') else ''
            columns = ', '.join(index['column_names'])
            index_ddl.append(f"CREATE {unique}INDEX {index['name']} ON {table_name} ({columns});")
        
        return index_ddl
        
    except Exception as e:
        print(f"Error getting indexes: {e}")
        return []

def get_complete_table_ddl(engine, table_name, schema=None):
    """
    Get complete DDL including partitions, comments, indexes, and constraints
    """
    ddl = get_table_ddl_with_partitions(engine, table_name, schema)
    
    if not ddl:
        return None
    
    # Add foreign key constraints
    try:
        inspector = inspect(engine)
        foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
        
        fk_ddl = []
        for fk in foreign_keys:
            cols = ', '.join(fk['constrained_columns'])
            ref_cols = ', '.join(fk['referred_columns'])
            fk_ddl.append(
                f"ALTER TABLE {table_name} ADD CONSTRAINT {fk['name']} "
                f"FOREIGN KEY ({cols}) REFERENCES {fk['referred_table']}({ref_cols});"
            )
        
        if fk_ddl:
            ddl += '\n\n' + '\n'.join(fk_ddl)
            
    except Exception as e:
        print(f"Error getting foreign keys: {e}")
    
    return ddl
import argparse
import psycopg2

from collections import namedtuple
from psycopg2.extras import NamedTupleCursor

def print_ex(ex):
    print(type(ex))
    print(ex.args)
    print(ex)


def execute_query(conn, sql_str, args=()):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_str, args)
        cursor.close()
    except Exception as ex:
        print_ex(ex)

        conn.close()

        exit(1)

class PostgresQL:
    def __init__(self,**kwargs):
        self.adress = kwargs['adress']
        self.port = kwargs['port']
        self.database = kwargs['database']
        self.user = kwargs['user']
        self.password = kwargs['password']
        self.pg_conn = None


    def connect(self):
        try:
            self.pg_conn = psycopg2.connect(dbname=self.database, user=self.user, password=self.password,
                                       host=self.adress, port=self.port)
            self.pg_conn.autocommit = True

            return True
        except Exception as ex:
            print_ex(ex)

            self.pg_conn = None

            return False

    def disconnect(self):
        try:
            self.pg_conn.close()

            return True
        except Exception as ex:
            print_ex(ex)

            return False

    def execute_query(self,sql_str,values):
            try:
                with self.pg_conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
                    cursor.execute(sql_str, tuple(values))

                    return cursor.fetchall()
            except Exception as ex:
                print_ex(ex)

                return None

    def select_and(self,table_name,**kwargs):
        sql_str = 'select * from '+table_name

        values = []

        if kwargs:
            where = []

            for key, value in kwargs.items():
                if value:
                    values.append(value)

                    where.append(f'({key}=%s)')
                else:
                    where.append(f'({key} is null)')

            sql_str += ' where '+'AND'.join(where)

        sql_str += ';'

        return self.execute_query(sql_str,values)

    def find(self,table_name, value):
        return self.select_and(table_name,id=value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--adress', required=False, type=str, default='localhost')
    parser.add_argument('-p', '--port', required=False, type=str, default='5432')
    parser.add_argument('-d', '--database', required=False, type=str, default='dataterminal')
    parser.add_argument('-u', '--user', required=False, type=str, default='dataterminal')
    parser.add_argument('-pas', '--password', required=False, type=str, default='terminal')
    parser.add_argument('-r', '--replication', required=False, type=str, default='publisher',
                        choices=['publisher', 'subscriber'])
    parser.add_argument('-n', '--node', required=False, type=str, default='local', choices=['local', 'global'])
    parser.add_argument('-t', '--terminal', required=True, type=str, default='')
    parser.add_argument('-ap', '--adress_pub', required=False, type=str, default='localhost')
    parser.add_argument('-pp', '--port_pub', required=False, type=str, default='5432')
    parser.add_argument('-dp', '--database_pub', required=False, type=str, default='dataterminal')
    parser.add_argument('-up', '--user_pub', required=False, type=str, default='dataterminal')
    parser.add_argument('-pasp', '--password_pub', required=False, type=str, default='terminal')

    namespace = parser.parse_args()

    try:
        pg_conn = psycopg2.connect(dbname=namespace.database, user=namespace.user, password=namespace.password,
                                   host=namespace.adress, port=namespace.port)
        pg_conn.autocommit = True
    except Exception as ex:
        print_ex(ex)

        exit(1)

    if namespace.replication == 'subscriber':
        sql_str = """
            DROP SUBSCRIPTION IF EXISTS dt_subscription_export_plan_%(terminal)s;
        """ % {'terminal': namespace.terminal}
        execute_query(pg_conn, sql_str)

        sql_str = """
            DROP SUBSCRIPTION IF EXISTS dt_subscription_doc_works_%(terminal)s;
        """ % {'terminal': namespace.terminal}
        execute_query(pg_conn, sql_str)

        if namespace.node == 'local':
            sql_str = """
                DROP SUBSCRIPTION IF EXISTS dt_subscription_doc_tasks_%(terminal)s;
            """ % {'terminal': namespace.terminal}
            execute_query(pg_conn, sql_str)

            sql_str = """
                DROP SUBSCRIPTION IF EXISTS dt_subscription_terminals_%(terminal)s;
            """ % {'terminal': namespace.terminal}
            execute_query(pg_conn, sql_str)
        # else:
        #     sql_str = "DROP SUBSCRIPTION IF EXISTS dt_subscription_doc_works_%s;" % (namespace.terminal)
        #
        #     execute_query(pg_conn, sql_str)

        con_str = "host=%s port=%s user=%s password=%s dbname=%s" % (
        namespace.adress_pub, namespace.port_pub, namespace.user_pub, namespace.password_pub, namespace.database_pub)

        sql_str = "CREATE SUBSCRIPTION dt_subscription_doc_works_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_publication_doc_works;" % {
            'con_str': con_str, 'terminal': namespace.terminal}

        execute_query(pg_conn, sql_str)

        sql_str = "CREATE SUBSCRIPTION dt_subscription_export_plan_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_publication_export_plan;" % {
            'con_str': con_str, 'terminal': namespace.terminal}

        execute_query(pg_conn, sql_str)

        if namespace.node == 'local':
            sql_str = "CREATE SUBSCRIPTION dt_subscription_doc_tasks_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_publication_doc_tasks;" % {
                'con_str': con_str, 'terminal': namespace.terminal}

            execute_query(pg_conn, sql_str)

            sql_str = "CREATE SUBSCRIPTION dt_subscription_terminals_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_publication_terminals;" % {
                'con_str': con_str, 'terminal': namespace.terminal}

            execute_query(pg_conn, sql_str)
        # elif namespace.node == 'global':
        #     sql_str = "CREATE SUBSCRIPTION dt_subscription_doc_works_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_publication_doc_works;" % {'con_str': con_str, 'terminal': namespace.terminal}
        #
        #     execute_query(pg_conn, sql_str)
    elif namespace.replication == 'publisher':
        sql_str = """
            DROP PUBLICATION IF EXISTS dt_publication_terminals;
            DROP PUBLICATION IF EXISTS dt_publication_doc_works;
            DROP PUBLICATION IF EXISTS dt_publication_doc_tasks;
            DROP PUBLICATION IF EXISTS dt_publication_export_plan;

            DROP TRIGGER IF EXISTS dt_set_id ON dt_doc_works CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_doc_tasks CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_terminals CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_special_codes CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works_items_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works_items_intervals CASCADE;
            DROP TRIGGER IF EXISTS dt_set_row_number ON dt_doc_works_items_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_reorder_row_number ON dt_doc_works_items_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_set_row_number ON dt_doc_works_items_intervals CASCADE;
            DROP TRIGGER IF EXISTS dt_reorder_row_number ON dt_doc_works_items_intervals CASCADE;
          
            DROP FUNCTION IF EXISTS dt_set_id() CASCADE;
            DROP FUNCTION IF EXISTS dt_export_plan() CASCADE;
            DROP FUNCTION IF EXISTS dt_set_row_number() CASCADE;
            DROP FUNCTION IF EXISTS dt_reorder_row_number() CASCADE;

            DROP TABLE IF EXISTS dt_export_plan CASCADE;
            DROP TABLE IF EXISTS dt_doc_works_items_intervals CASCADE;
            DROP TABLE IF EXISTS dt_doc_works_items_operators CASCADE;
            DROP TABLE IF EXISTS dt_doc_works CASCADE;
            DROP TABLE IF EXISTS dt_doc_tasks_items_terminals CASCADE;
            DROP TABLE IF EXISTS dt_doc_tasks CASCADE;
            DROP TABLE IF EXISTS dt_special_codes CASCADE;
            DROP TABLE IF EXISTS dt_operators CASCADE;
            DROP TABLE IF EXISTS dt_terminals CASCADE;

            DROP TYPE IF EXISTS dt_status;
            DROP TYPE IF EXISTS dt_terminal_id;

            DROP EXTENSION IF EXISTS pgcrypto;
            
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
            
            CREATE TYPE dt_status AS ENUM ('work', 'stop', 'complete');
            CREATE TYPE dt_terminal_id AS ENUM (%(terminal)s);

            CREATE TABLE dt_terminals (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL);

            CREATE TABLE dt_operators (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL);

            CREATE TABLE dt_special_codes (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL);

            CREATE TABLE dt_doc_tasks (
            id uuid PRIMARY KEY,
            doc_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            doc_number varchar(12) NOT NULL UNIQUE);

            CREATE TABLE dt_doc_tasks_items_terminals (
            position integer NOT NULL,
            id uuid references dt_doc_tasks(id) ON DELETE CASCADE NOT NULL,
            dt_terminals_id uuid references dt_terminals(id) ON DELETE RESTRICT NOT NULL,
            PRIMARY KEY (position, id));

            CREATE TABLE dt_doc_works (
            id uuid PRIMARY KEY,
            doc_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            dt_doc_tasks_id uuid references dt_doc_tasks(id) ON DELETE CASCADE,
            doc_status dt_status DEFAULT 'work' NOT NULL);

            CREATE TABLE dt_doc_works_items_operators (
            position integer NOT NULL,
            id uuid references dt_doc_works(id) ON DELETE CASCADE NOT NULL,
            dt_operators_id uuid references dt_operators(id) ON DELETE RESTRICT NOT NULL,
            PRIMARY KEY (position, id));

            CREATE TABLE dt_doc_works_items_intervals (
            position integer NOT NULL,
            id uuid references dt_doc_works(id) ON DELETE CASCADE NOT NULL,
            begin_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            end_timestamp timestamp with time zone DEFAULT NULL,
            work_interval integer DEFAULT 0,
            stop_interval integer DEFAULT 0,
            PRIMARY KEY (position, id));
            
            CREATE TABLE dt_export_plan (
            id uuid NOT NULL,
            table_name text NOT NULL,
            action char(6) NOT NULL,
            action_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(id,table_name));
                
            CREATE OR REPLACE FUNCTION dt_export_plan() RETURNS trigger AS $dt_export_plan$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO dt_export_plan (id,table_name,action,action_timestamp) VALUES (NEW.id,TG_TABLE_NAME,TG_OP,now()) ON CONFLICT (id,table_name) DO UPDATE SET action=TG_OP,action_timestamp=now();
                ELSE
                    INSERT INTO dt_export_plan (id,table_name,action,action_timestamp) VALUES (OLD.id,TG_TABLE_NAME,TG_OP,now()) ON CONFLICT (id,table_name) DO UPDATE SET action=TG_OP,action_timestamp=now();
                END IF;
              
                RETURN NULL;
            END;
            $dt_export_plan$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE OR DELETE ON dt_doc_works
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE OR DELETE ON dt_doc_works_items_operators
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE OR DELETE ON dt_doc_works_items_intervals
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();
                
            CREATE OR REPLACE FUNCTION dt_set_row_number() RETURNS trigger AS $dt_set_row_number$
            DECLARE
              ids RECORD;
            BEGIN
              FOR ids IN EXECUTE 'SELECT max(position) AS max_pos FROM '|| TG_TABLE_NAME ||' WHERE id=$1' USING NEW.id LOOP
                IF ids.max_pos IS NOT NULL THEN
                  NEW.position := ids.max_pos+1;
                END IF;
              END LOOP;

              IF NEW.position IS NULL THEN
                NEW.position := 1;
              END IF;

              RETURN NEW;
            END;
            $dt_set_row_number$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_set_row_number BEFORE INSERT ON dt_doc_works_items_operators
            FOR EACH ROW EXECUTE PROCEDURE dt_set_row_number();          

            CREATE TRIGGER dt_set_row_number BEFORE INSERT ON dt_doc_works_items_intervals
            FOR EACH ROW EXECUTE PROCEDURE dt_set_row_number();
            
            CREATE OR REPLACE FUNCTION dt_reorder_row_number() RETURNS trigger AS $dt_reorder_row_number$
            DECLARE
              ids RECORD;
            BEGIN
              FOR ids IN EXECUTE 'SELECT position FROM '|| TG_TABLE_NAME ||' WHERE (id=$1) AND (position>$2) ORDER BY position' USING OLD.id,OLD.position LOOP
                EXECUTE 'UPDATE ' || TG_TABLE_NAME || ' set position=($1-1) WHERE (id=$2) AND (position=$1)' USING ids.position,OLD.id;
              END LOOP;

              RETURN NEW;
            END;
            $dt_reorder_row_number$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_reorder_row_number AFTER DELETE ON dt_doc_works_items_operators
            FOR EACH ROW EXECUTE PROCEDURE dt_reorder_row_number();
          
            CREATE TRIGGER dt_reorder_row_number AFTER DELETE ON dt_doc_works_items_intervals
            FOR EACH ROW EXECUTE PROCEDURE dt_reorder_row_number();            

            CREATE OR REPLACE FUNCTION dt_set_id() RETURNS trigger AS $dt_set_id$
            BEGIN
              IF NEW.id IS NULL THEN
                NEW.id = gen_random_uuid();
              END IF;

              RETURN NEW;
            END;
            $dt_set_id$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_doc_works
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_doc_tasks
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_operators
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_terminals
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_special_codes
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();
        """

        if namespace.node == 'local':
            sql_str += """
                CREATE PUBLICATION dt_publication_doc_works FOR TABLE dt_doc_works, dt_doc_works_items_operators, dt_doc_works_items_intervals;
                CREATE PUBLICATION dt_publication_export_plan FOR TABLE dt_export_plan WITH (publish = 'insert,update');
            """
        elif namespace.node == 'global':
            sql_str += """
                CREATE PUBLICATION dt_publication_doc_tasks FOR TABLE dt_doc_tasks, dt_doc_tasks_items_terminals;
                CREATE PUBLICATION dt_publication_doc_works FOR TABLE dt_doc_works, dt_doc_works_items_operators, dt_doc_works_items_intervals WITH (publish = 'delete,truncate');
                CREATE PUBLICATION dt_publication_terminals FOR TABLE dt_terminals, dt_operators, dt_special_codes;
                CREATE PUBLICATION dt_publication_export_plan FOR TABLE dt_export_plan WITH (publish = 'delete,truncate');
            """

        execute_query(pg_conn, sql_str, {'terminal': namespace.terminal})

    try:
        pg_conn.close()
    except Exception as ex:
        print_ex(ex)


Dbms = PostgresQL(adress='localhost',port='5432',database='dataterminal',user='dataterminal',password='terminal')

if __name__ == '__main__':
    main()

import argparse
import uuid

import psycopg2
import psycopg2.extras

psycopg2.extras.register_uuid()

def print_ex(ex):
    print(type(ex))
    print(ex.args)
    print(ex)

def get_new_dbms(**kwargs):
    return PostgresQL(**kwargs)

class DbmsConnected(Exception):
    def __init__(self):
        self.message = 'Підключення до бази даних відкрите'

        super().__init__(self.message)


class PostgresQL:
    CONNECT_PARAMS = (
    'adress', 'port', 'dbname', 'user', 'password', 'adress_pub', 'port_pub', 'dbname_pub', 'user_pub', 'password_pub')

    def __init__(self, **kwargs):
        self.set_dbms_attribute(**kwargs)

        self.pg_conn = None

    def connect(self):
        try:
            self.pg_conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password,
                                            host=self.adress, port=self.port, cursor_factory=psycopg2.extras.DictCursor)
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

    def connected(self):
        try:
            if self.pg_conn.closed == 0:
                return True
            else:
                return False
        except Exception as ex:
            print_ex(ex)

            return False

    def set_dbms_attribute(self, **kwargs):
        for key, value in kwargs.items():
            if key in PostgresQL.CONNECT_PARAMS:
                setattr(self, key, value)

    def execute_query(self, sql_str, values=()):
        try:
            cursor = self.pg_conn.cursor()

            cursor.execute(sql_str, tuple(values))
        except Exception as ex:
            print_ex(ex)

            return False

        try:
            return cursor.fetchall()
        except:
            return None
        finally:
            cursor.close()

    def set_terminal_id(self, terminal_id):
        sql_str = """
            DROP TYPE IF EXISTS dt_terminal_id;
        """
        self.execute_query(sql_str)

        sql_str = """
            CREATE TYPE dt_terminal_id AS ENUM ('%(terminal)s');
        """ % {'terminal': terminal_id}

        self.execute_query(sql_str)

        return True

    def generate_terminal_id(self):
        sql_str = """
            select * from gen_random_uuid();
        """
        res = self.execute_query(sql_str)

        if res:
            self.set_terminal_id(res[0]['gen_random_uuid'])

        return True

    def get_terminal_id(self):
        sql_str = """
            select enum_range(null::dt_terminal_id);
        """

        res = self.execute_query(sql_str)

        if res:
            return res[0]['enum_range'][1:-1]
        else:
            return res

    def delete_publications_global(self):
        sql_str = """
            DROP PUBLICATION IF EXISTS dt_pub_global;
            DROP PUBLICATION IF EXISTS dt_pub_ep;
        """

        self.execute_query(sql_str)

        return True

    def delete_publications_global_row_filter(self, terminal_id):
        sql_str = f"""
            DROP PUBLICATION IF EXISTS dt_pub_{terminal_id};
        """

        self.execute_query(sql_str)

        return True

    def create_publications_global(self):
        sql_str = f"""
            CREATE PUBLICATION dt_pub_global FOR TABLE dt_doc_tasks, dt_operators, dt_special_codes, dt_equipments;
            CREATE PUBLICATION dt_pub_ep FOR TABLE dt_export_plan WITH (publish = 'delete,truncate');
        """

        self.execute_query(sql_str)

        return True

    def create_publications_global_row_filter(self, terminal_id):
        sql_str = f"""
            CREATE PUBLICATION dt_pub_{terminal_id} FOR TABLE dt_terminals WHERE (id=%s);
        """

        self.execute_query(sql_str,[uuid.UUID(f'{terminal_id}')])

        return True

    def delete_publications_local(self):
        sql_str = """
            DROP PUBLICATION IF EXISTS dt_pub_data;
        """

        self.execute_query(sql_str)

        return True

    def create_publications_local(self):
        sql_str = """
            CREATE PUBLICATION dt_pub_data FOR TABLE dt_terminals, dt_export_plan, dt_doc_works, dt_doc_works_items_operators, dt_doc_works_items_intervals;
        """

        self.execute_query(sql_str)

        return True

    def create_subscriptions_global(self, terminal_id):
        con_str = "host=%s port=%s user=%s password=%s dbname=%s" % (
            self.adress_pub, self.port_pub, self.user_pub, self.password_pub,
            self.dbname_pub)

        sql_str = "CREATE SUBSCRIPTION dt_sub_%(terminal)s CONNECTION '%(con_str)s' PUBLICATION dt_pub_data WITH (copy_data = false, origin = none);" % {
            'con_str': con_str, 'terminal': terminal_id}

        self.execute_query(sql_str)

        return True

    def delete_subscriptions_global(self, terminal_id):
        sql_str = f"""
            DROP SUBSCRIPTION IF EXISTS dt_sub_{terminal_id};
        """

        self.execute_query(sql_str)

        return True

    def delete_subscriptions_local(self):
        res = self.get_terminal_id()

        if res:
            terminal_id = res.replace('-', '')

            sql_str = f"""
                DROP SUBSCRIPTION IF EXISTS dt_sub_global_{terminal_id};
            """

            self.execute_query(sql_str)

            sql_str = f"""
                DROP SUBSCRIPTION IF EXISTS dt_sub_ep_{terminal_id};
            """

            self.execute_query(sql_str)

            sql_str = f"""
                DROP SUBSCRIPTION IF EXISTS dt_sub_{terminal_id};
            """

            self.execute_query(sql_str)

        return True

    def create_subscriptions_local(self):
        res = self.get_terminal_id()

        if res:
            terminal_id = res.replace('-', '')

            con_str = "host=%s port=%s user=%s password=%s dbname=%s" % (
            self.adress_pub, self.port_pub, self.user_pub, self.password_pub, self.dbname_pub)

            sql_str = f"""
                CREATE SUBSCRIPTION dt_sub_global_{terminal_id} CONNECTION '{con_str}' PUBLICATION dt_pub_global WITH (copy_data = true, origin = none);
            """

            self.execute_query(sql_str)

            sql_str = f"""
                CREATE SUBSCRIPTION dt_sub_ep_{terminal_id} CONNECTION '{con_str}' PUBLICATION dt_pub_ep WITH (copy_data = false, origin = none);
            """

            self.execute_query(sql_str)

            sql_str = f"""
                CREATE SUBSCRIPTION dt_sub_{terminal_id} CONNECTION '{con_str}' PUBLICATION dt_pub_{terminal_id} WITH (copy_data = false, origin = none);
            """

            self.execute_query(sql_str)

        return True

    def push_replications_to_global(self, **kwargs):
        res = self.get_terminal_id()

        if res:
            dbms = get_new_dbms(**kwargs)

            dbms.connect()

            dbms.create_publications_global_row_filter(res.replace('-', ''))

            dbms.create_subscriptions_global(res.replace('-', ''))

            dbms.disconnect()

        return True

    def pop_replications_from_global(self, **kwargs):
        res = self.get_terminal_id()

        if res:
            dbms = get_new_dbms(**kwargs)

            dbms.connect()

            dbms.delete_publications_global_row_filter(res.replace('-', ''))

            dbms.delete_subscriptions_global(res.replace('-', ''))

            dbms.disconnect()

        return True

    def push_subscriptions_to_local(self, **kwargs):
        dbms = PostgresQL(**kwargs)

        dbms.connect()

        dbms.create_subscriptions_local()

        dbms.disconnect()

        return True

    def pop_subscriptions_from_local(self, **kwargs):
        dbms = PostgresQL(**kwargs)

        dbms.connect()

        dbms.delete_subscriptions_local()

        dbms.disconnect()

        return True

    def create_tables(self):
        sql_str = """
            DROP TRIGGER IF EXISTS dt_set_status ON dt_doc_tasks CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_doc_works CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_doc_tasks CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_terminals CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_special_codes CASCADE;
            DROP TRIGGER IF EXISTS dt_set_id ON dt_equipments CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_tasks CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works_items_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_export_plan ON dt_doc_works_items_intervals CASCADE;
            DROP TRIGGER IF EXISTS dt_reorder_row_number ON dt_doc_works_items_operators CASCADE;
            DROP TRIGGER IF EXISTS dt_reorder_row_number ON dt_doc_works_items_intervals CASCADE;

            DROP FUNCTION IF EXISTS dt_set_status() CASCADE;
            DROP FUNCTION IF EXISTS dt_set_id() CASCADE;
            DROP FUNCTION IF EXISTS dt_export_plan() CASCADE;
            DROP FUNCTION IF EXISTS dt_reorder_row_number() CASCADE;

            DROP TABLE IF EXISTS dt_export_plan CASCADE;
            DROP TABLE IF EXISTS dt_terminals CASCADE;
            DROP TABLE IF EXISTS dt_doc_works_items_intervals CASCADE;
            DROP TABLE IF EXISTS dt_doc_works_items_operators CASCADE;
            DROP TABLE IF EXISTS dt_doc_works CASCADE;
            DROP TABLE IF EXISTS dt_doc_tasks CASCADE;
            DROP TABLE IF EXISTS dt_special_codes CASCADE;
            DROP TABLE IF EXISTS dt_operators CASCADE;
            DROP TABLE IF EXISTS dt_equipments CASCADE;

            DROP TYPE IF EXISTS dt_status;

            DROP EXTENSION IF EXISTS pgcrypto;

            CREATE EXTENSION IF NOT EXISTS pgcrypto;

            CREATE TYPE dt_status AS ENUM ('work', 'paused', 'not_finished', 'complete');

            CREATE TABLE dt_equipments (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL,
            line_id uuid);

            CREATE TABLE dt_operators (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL);

            CREATE TABLE dt_special_codes (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL);

            CREATE TABLE dt_doc_tasks (
            id uuid PRIMARY KEY,
            doc_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            doc_number varchar(12) NOT NULL UNIQUE,
            line_id uuid,
            status boolean DEFAULT False NOT NULL);

            CREATE TABLE dt_doc_works (
            id uuid PRIMARY KEY,
            doc_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            dt_doc_tasks_id uuid references dt_doc_tasks(id) ON DELETE CASCADE,
            dt_equipments_id uuid references dt_equipments(id) ON DELETE RESTRICT NOT NULL,
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

            CREATE TABLE dt_terminals (
            id uuid PRIMARY KEY,
            name varchar(120) NOT NULL,
            dt_equipments_id uuid references dt_equipments(id) ON DELETE RESTRICT DEFAULT NULL,
            dt_doc_tasks_id uuid references dt_doc_tasks(id) ON DELETE RESTRICT DEFAULT NULL,
            dt_doc_works_id uuid references dt_doc_works(id) ON DELETE RESTRICT DEFAULT NULL,
            last_seen timestamp with time zone DEFAULT NULL);
            
            CREATE TABLE dt_export_plan (
            id uuid NOT NULL,
            table_name text NOT NULL,
            action char(6) NOT NULL,
            action_timestamp timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
            PRIMARY KEY(id,table_name));

            CREATE OR REPLACE FUNCTION dt_export_plan() RETURNS trigger AS $dt_export_plan$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    INSERT INTO public.dt_export_plan (id,table_name,action,action_timestamp) VALUES (NEW.id,TG_TABLE_NAME,TG_OP,now()) ON CONFLICT (id,table_name) DO UPDATE SET action=TG_OP,action_timestamp=now();
                ELSE
                    INSERT INTO public.dt_export_plan (id,table_name,action,action_timestamp) VALUES (OLD.id,TG_TABLE_NAME,TG_OP,now()) ON CONFLICT (id,table_name) DO UPDATE SET action=TG_OP,action_timestamp=now();
                END IF;

                RETURN NULL;
            END;
            $dt_export_plan$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_export_plan AFTER UPDATE ON dt_doc_tasks
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE ON dt_doc_works
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE ON dt_doc_works_items_operators
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

            CREATE TRIGGER dt_export_plan AFTER INSERT OR UPDATE ON dt_doc_works_items_intervals
            FOR EACH ROW EXECUTE PROCEDURE dt_export_plan();

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
                    NEW.id := gen_random_uuid();
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

            CREATE TRIGGER dt_set_id BEFORE INSERT ON dt_equipments
            FOR EACH ROW EXECUTE PROCEDURE dt_set_id();

            CREATE OR REPLACE FUNCTION dt_set_status() RETURNS trigger AS $dt_set_status$
            DECLARE
              ids RECORD;
            BEGIN
              IF NEW.doc_status = 'complete' THEN
                    EXECUTE 'UPDATE public.dt_doc_tasks set status = True WHERE (id=$1)' USING NEW.dt_doc_tasks_id;
              END IF;

              RETURN NEW;
            END;
            $dt_set_status$ LANGUAGE plpgsql;

            CREATE TRIGGER dt_set_status AFTER INSERT OR UPDATE ON dt_doc_works
            FOR EACH ROW EXECUTE PROCEDURE dt_set_status();
            
            ALTER TABLE dt_doc_works ENABLE ALWAYS TRIGGER dt_set_status;
        """

        self.execute_query(sql_str)

        return True

    def create_default_terminal(self):
        sql_str = """
            INSERT INTO public.dt_terminals(id, name, last_seen) VALUES (%s, %s, now());
        """

        self.execute_query(sql_str, (self.get_terminal_id(), 'Новий термінал'))

        return True

    def select_and(self, table_name, args=None):
        sql_str = 'select * from ' + table_name

        values = []

        if args:
            where = []

            for key, value in args.items():
                if value:
                    values.append(value)

                    where.append(f'({key}=%s)')
                else:
                    where.append(f'({key} is null)')

            sql_str += ' where ' + 'AND'.join(where)

        sql_str += ';'

        return self.execute_query(sql_str, values)

    def find(self, table_name, args):
        return self.select_and(table_name, args)

    def insert_update(self,table_name, requisites={}, conflict=[]):
        sql_str = 'insert into ' + table_name

        cols = []
        vals = []
        columns_update = []

        if requisites:
            for key, value in requisites.items():
                cols.append(key)
                vals.append(value)
                columns_update.append(f'{key}=%s')

        sql_str += '('+','.join(cols)+') VALUES ('+','.join(['%s']*len(vals))+')'

        if len(conflict) > 0:
            sql_str += ' ON CONFLICT ('+','.join(conflict)+') DO UPDATE SET '+','.join(columns_update)
            vals = vals*2

        sql_str += ';'

        return self.execute_query(sql_str, vals)

    def items_count(self, table_name, value):
        sql_str = f'SELECT max(position) AS max_pos FROM {table_name} WHERE id=%s'

        return self.execute_query(sql_str, [value])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--run', required=True, type=str, default='publisher',
                        choices=['delete_subscriptions', 'delete_publications', 'delete_publications_row_filter', 'create_publications',
                                 'create_publications_row_filter', 'create_subscriptions', 'push_replications', 'pop_replications', 'create_tables',
                                 'create_default_terminal'])
    parser.add_argument('-n', '--node', required=True, type=str, default='local', choices=['local', 'global'])
    parser.add_argument('-t', '--terminal', required=False, type=str, default='')
    parser.add_argument('-a', '--adress', required=False, type=str, default='localhost')
    parser.add_argument('-p', '--port', required=False, type=str, default='5432')
    parser.add_argument('-d', '--dbname', required=False, type=str, default='dataterminal')
    parser.add_argument('-u', '--user', required=False, type=str, default='dataterminal')
    parser.add_argument('-pas', '--password', required=False, type=str, default='terminal')
    parser.add_argument('-ap', '--adress_pub', required=False, type=str, default='')
    parser.add_argument('-pp', '--port_pub', required=False, type=str, default='')
    parser.add_argument('-dp', '--dbname_pub', required=False, type=str, default='dataterminal')
    parser.add_argument('-up', '--user_pub', required=False, type=str, default='dataterminal')
    parser.add_argument('-pasp', '--password_pub', required=False, type=str, default='terminal')
    parser.add_argument('-ra', '--adress_remote', required=False, type=str, default='')
    parser.add_argument('-rp', '--port_remote', required=False, type=str, default='')
    parser.add_argument('-rd', '--dbname_remote', required=False, type=str, default='dataterminal')
    parser.add_argument('-ru', '--user_remote', required=False, type=str, default='dataterminal')
    parser.add_argument('-rpas', '--password_remote', required=False, type=str, default='terminal')

    namespace = parser.parse_args()

    dbms = PostgresQL()
    dbms.set_dbms_attribute(adress=namespace.adress, port=namespace.port, dbname=namespace.dbname,
                            user=namespace.user, password=namespace.password)

    dbms.set_dbms_attribute(adress_pub=namespace.adress_pub, port_pub=namespace.port_pub,
                            dbname_pub=namespace.dbname_pub,
                            user_pub=namespace.user_pub, password_pub=namespace.password_pub)

    dbms.connect()

    match namespace.run:
        case 'delete_subscriptions':
            match namespace.node:
                case 'local':
                    dbms.delete_subscriptions_local()
                case 'global':
                    if len(namespace.terminal) > 0:
                        dbms.delete_subscriptions_global(namespace.terminal)
                    else:
                        print(
                            'При видаленні підписки в глобальній базі даних необхідно вказати унікальну назву терміналу (ключ -t)')
        case 'delete_publications':
            match namespace.node:
                case 'local':
                    dbms.delete_publications_local()
                case 'global':
                    dbms.delete_publications_global()
        case 'delete_publications_row_filter':
            match namespace.node:
                case 'local':
                    print('Видаляти публікації з фільтром по строках можна тільки в глобальній БД')
                case 'global':
                    dbms.delete_publications_global_row_filter(namespace.terminal)
        case 'create_subscriptions':
            match namespace.node:
                case 'local':
                    dbms.create_subscriptions_local()
                case 'global':
                    if len(namespace.terminal) > 0:
                        dbms.create_subscriptions_global(namespace.terminal)
                    else:
                        print(
                            'При створенні підписки в глобальній базі даних необхідно вказати унікальну назву терміналу (ключ -t)')
        case 'push_replications':
            match namespace.node:
                case 'local':
                    dbms.push_replications_to_global(adress=namespace.adress_remote, port=namespace.port_remote,
                                                      dbname=namespace.dbname_remote, user=namespace.user_remote,
                                                      password=namespace.password_remote,
                                                      adress_pub=namespace.adress_pub, port_pub=namespace.port_pub,
                                                      dbname_pub=namespace.dbname_pub, user_pub=namespace.user_pub,
                                                      password_pub=namespace.password_pub)
                case 'global':
                    dbms.push_subscriptions_to_local(adress=namespace.adress_remote, port=namespace.port_remote,
                                                     dbname=namespace.dbname_remote, user=namespace.user_remote,
                                                     password=namespace.password_remote,
                                                     adress_pub=namespace.adress_pub, port_pub=namespace.port_pub,
                                                     dbname_pub=namespace.dbname_pub, user_pub=namespace.user_pub,
                                                     password_pub=namespace.password_pub)
        case 'pop_replications':
            match namespace.node:
                case 'local':
                    dbms.pop_replications_from_global(adress=namespace.adress_remote, port=namespace.port_remote,
                                                       dbname=namespace.dbname_remote, user=namespace.user_remote,
                                                       password=namespace.password_remote,
                                                       adress_pub=namespace.adress_pub, port_pub=namespace.port_pub,
                                                       dbname_pub=namespace.dbname_pub, user_pub=namespace.user_pub,
                                                       password_pub=namespace.password_pub)
                case 'global':
                    dbms.pop_subscriptions_from_local(adress=namespace.adress_remote, port=namespace.port_remote,
                                                      dbname=namespace.dbname_remote, user=namespace.user_remote,
                                                      password=namespace.password_remote,
                                                      adress_pub=namespace.adress_pub, port_pub=namespace.port_pub,
                                                      dbname_pub=namespace.dbname_pub, user_pub=namespace.user_pub,
                                                      password_pub=namespace.password_pub)
        case 'create_publications':
            match namespace.node:
                case 'local':
                    dbms.create_publications_local()
                case 'global':
                    dbms.create_publications_global()
        case 'create_publications_row_filter':
            match namespace.node:
                case 'local':
                    print('Створювати публікації з фільтром по строках можна тільки в глобальній БД')
                case 'global':
                    dbms.create_publications_global_row_filter(namespace.terminal)
        case 'create_tables':
            if len(namespace.terminal) > 0:
                dbms.set_terminal_id(namespace.terminal)
            else:
                dbms.generate_terminal_id()

            dbms.create_tables()
        case 'create_default_terminal':
            dbms.create_default_terminal()

    dbms.disconnect()

if __name__ == '__main__':
    main()
else:
    Dbms = get_new_dbms(adress='localhost', port='5432', dbname='dataterminal', user='dataterminal',
                      password='terminal')

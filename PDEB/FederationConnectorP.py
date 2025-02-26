import psycopg2
import sys

class FederationConnectorP:
    cursors = dict()
    cons = dict()
    federations = dict()

    def create_federation_connection(self):
        #Here we provide the connection information for a federaiton with four nodes in PostgreSQL FDW. Please add if needed. please setup the connection.
        self.cons[1] = psycopg2.connect(
            host="",
            dbname='',
            user='',
            password='',
            port=
        )
        self.cons[2] = psycopg2.connect(
            host="",
            dbname='',
            user='',
            password='',
            port=
        )

        self.cons[3] = psycopg2.connect(
            host="",
            dbname='',
            user='',
            password='',
            port=
        )

        self.cons[4] = psycopg2.connect(
            host="",
            dbname='',
            user='',
            password='',
            port=
        )

        # self.cons[1].commit()
        self.cons[1].autocommit = True
        self.cons[2].autocommit = True
        self.cons[3].autocommit = True
        self.cons[4].autocommit = True

        self.cursors[1] = self.cons[1].cursor()
        self.cursors[2] = self.cons[2].cursor()
        self.cursors[3] = self.cons[3].cursor()
        self.cursors[4] = self.cons[4].cursor()

    def disconnect_federation(self):
        for con_id in self.cons:
            try:
                self.cursors.get(con_id).close()
                self.cons.get(con_id).close()
            except Exception as error:
                print("Unexpected error:", error, sys.exc_info()[0], " @FD"+str(con_id))
        print("All connections are closed")

    def execute_query(self, site_id, query, show_result=False):
        try:
            self.cursors.get(site_id).execute(query)
            if show_result:
                try:
                    rows = self.cursors.get(site_id).fetchall()
                    if rows:
                        print(rows)
                except Exception as error:
                    print("Fetching error:", sys.exc_info()[0], " @site"+str(site_id)+": "+query+"\n")
                    pass
        except Exception as error:
            print("Unexpected error:", error, sys.exc_info()[0], " @site"+str(site_id)+": "+query)



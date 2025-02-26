import pyhdb
import sys


class FederationConnector:

    cursors = dict()
    cons = dict()
    federations = dict()
    dropables = dict()

    def __init__(self):
        self.federations[1] = "DKE1_HANA.SystemDB.DKE1."
        self.federations[2] = "DKE2_HANA.SystemDB.DKE2."
        self.federations[3] = "DKE3_HANA.SystemDB.DKE3."
        self.federations[4] = "DKE4_HANA.SystemDB.DKE4."

    # Here we provide the connection information for a federaiton with four nodes in SAP HANA SDA. Please add if needed. please setup the connection.

    def create_federation_connection(self):
        self.cons[1] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[2] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[3] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[4] = pyhdb.connect(
            host="",
            port=,
            user="",
            password=""
        )
        self.cons[1].setautocommit(True)
        self.cons[2].setautocommit(True)
        self.cons[3].setautocommit(True)
        self.cons[4].setautocommit(True)
        self.cursors[1] = self.cons[1].cursor()
        self.cursors[2] = self.cons[2].cursor()
        self.cursors[3] = self.cons[3].cursor()
        self.cursors[4] = self.cons[4].cursor()


    def disconnect_federation(self):
        for con_id in self.cons:
            try:
                self.cons.get(con_id).close()
            except pyhdb.exceptions.Error:
                print("Unexpected error:", sys.exc_info()[0], " @FD"+str(con_id))
        print("All connections are closed")

    def add_to_dropables(self, federation_id, ddl):
        if federation_id not in self.dropables:
            self.dropables[federation_id] = list()
        if ddl not in self.dropables.get(federation_id):
            self.dropables.get(federation_id).append(ddl)

    def execute_query(self, site_id, query, show_result=False):
        try:
            res = self.cursors.get(site_id).execute(query)
            if show_result:
                try:
                    rows = res.fetchall()
                    if rows:
                        print("Result: {0}".format(str(rows)))
                except pyhdb.exceptions.Error:
                    pass
        except pyhdb.exceptions.Error:
            print("Unexpected error:", sys.exc_info()[0], " @EB"+str(site_id)+": "+query)

    def drop_dropables(self):
        for site_id in self.dropables:
            for tab_name in self.dropables.get(site_id):
                try:
                    self.cursors.get(site_id).execute("DROP VIEW "+tab_name+";")
                except pyhdb.exceptions.Error:
                    try:
                        self.cursors.get(site_id).execute("DROP TYPE " + tab_name + ";")
                    except pyhdb.exceptions.Error:
                        try:
                            self.cursors.get(site_id).execute("DROP TABLE "+tab_name+";")
                        except pyhdb.exceptions.Error:
                            try:
                                self.cursors.get(site_id).execute("DROP PROCEDURE " + tab_name + ";")
                            except pyhdb.exceptions.Error:
                                print("Failed to drop " + tab_name + " @EB" + str(site_id))
        self.dropables.clear()


    def getValue(self, site_id, query, show_result=False):
        dataType = 0

        try:
            res = self.cursors.get(site_id).execute(query)
            if show_result:
                try:
                    rows = res.fetchall()
                    if rows:
                        for row in rows:
                            dataType = row[0]
                except pyhdb.exceptions.Error:
                    print("Fetching error:", sys.exc_info()[0], " @EB"+str(site_id)+": "+query+"\n")
                    # pass
        except pyhdb.exceptions.Error:
            pass
        return dataType
"""
This script defines classes to authorise and call
the Tableau REST API 
"""
import tableauserverclient as TSC
from functools import partial
import pandas as pd

class tableauServer:
    def __init__(self,url,site,pat,patSecret):
        self.url = url
        self.site = site
        self.pat = pat
        self.patSecret = patSecret
        self.tokenAuth = TSC.PersonalAccessTokenAuth(self.pat,self.patSecret,self.site)
        self.server = TSC.Server(self.url,use_server_version=True)

    def auth(self):
        try:
            self.server.auth.sign_in(self.tokenAuth)
            print("Signin Successful")
        except Exception as e:
            print(e)

    def dfParse(self, objList):
        df = pd.DataFrame([vars(obj) for obj in objList])
        df["site"] = self.site
        return df

    def get_datasource_mappings(self):
        all_connections = []
        with self.server.auth.sign_in(self.tokenAuth):
            for wb in TSC.Pager(self.server.workbooks):
                self.server.workbooks.populate_connections(wb)
                all_connections.append({"workbook_id":wb.id,"datasource_name":[conn.datasource_name for conn in wb.connections]})
        return all_connections
        
    def get_workbooks(self):
        with self.server.auth.sign_in(self.tokenAuth):
            results = [wb for wb in TSC.Pager(self.server.workbooks)]
            return self.dfParse(results)
          
    def get_datasources(self):
        with self.server.auth.sign_in(self.tokenAuth):
            results = [wb for wb in TSC.Pager(self.server.datasources)]
            return self.dfParse(results)
        
    def get_users(self):
        with self.server.auth.sign_in(self.tokenAuth):
            results = [wb for wb in TSC.Pager(self.server.users)]
            return self.dfParse(results)
        
    def get_views(self):
        with self.server.auth.sign_in(self.tokenAuth):
            view_pager = partial(self.server.views.get,usage=True)
            results = [wb for wb in TSC.Pager(view_pager)]
            return self.dfParse(results)
        

"""
This script defines classes to authorise and call
the Tableau REST API 
"""
import tableauserverclient as TSC

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

    def get_datasource_mappings(self):
        all_connections = []
        with self.server.auth.sign_in(self.tokenAuth):
            for wb in TSC.Pager(self.server.workbooks):
                self.server.workbooks.populate_connections(wb)
                all_connections.append({"workbook_id":wb.id,"datasource_ids":[conn.datasource_id for conn in wb.connections]})
        return all_connections
    
    def get_view_mappings(self):
        all_views = []
        with self.server.auth.sign_in(self.tokenAuth):
            for wb in TSC.Pager(self.server.workbooks):
                self.server.workbooks.populate_views(wb)
                all_views.append({"workbook_id":wb.id,"view_ids":[view.id for view in wb.views]})
        return all_views
    
    def get_workbooks(self):
        with self.server.auth.sign_in(self.tokenAuth):
            return [wb for wb in TSC.Pager(self.server.workbooks)]
          
    def get_datasources(self):
        with self.server.auth.sign_in(self.tokenAuth):
            return [wb for wb in TSC.Pager(self.server.datasources)]
        
    def get_users(self):
        with self.server.auth.sign_in(self.tokenAuth):
            return [wb for wb in TSC.Pager(self.server.users)]
        
    def get_views(self):
        with self.server.auth.sign_in(self.tokenAuth):
            return [wb for wb in TSC.Pager(self.server.views)]
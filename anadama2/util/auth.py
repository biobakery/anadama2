# -*- coding: utf-8 -*-
class AuthInfo(object):
    def __init__(self, username, projectname, commit_id, auth_key):
        self.username    = username
        self.projectname = projectname
        self.commit_id   = commit_id
        self.auth_key    = auth_key


    @classmethod    
    def parse(cls, s):
        if not s:
            return None
        try:
            return cls(*s.split("_:_"))
        except:
            return None


    def to_dict(self):
        return {
            "username": self.username, "projectname": self.projectname,
            "commit_id": self.commit_id, "auth_key": self.auth_key
        }

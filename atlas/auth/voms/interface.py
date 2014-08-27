#!/bin/env python

from VOMSAdmin.VOMSCommands import VOMSAdminProxy

class VomsInterface:
    """
    Retrieving information from VOMS server.
    """

    def __init__(self, options):
        self.options = VomsInterface.get_identity_options()
        self.options.update(options)
        self.voms = VOMSAdminProxy(**options)

    def list_users(self):
        vo_users = self.voms.listUsers()
        users = []

        for user in vo_users:
            user_info = {}
            for field in "CA,DN,mail".split(","):
                user_info[field] = getattr(user, "_" + field)
            users.append(user_info)
        return users

    def list_user_attributes(self, dn, ca):
        """
        Read user's attributes from VOMS server
        :param dn: DN of user's certificate
        :param ca: DN of the issuer of the certificate (CA)
        :return: user's attributes as a dict
        """
        result = {}
        attrs = self.voms.call_method("list-user-attributes", dn, ca)
        for item in attrs:
            result[item._attributeClass._name] = item._value
        return result

    def list_user_groups(self, dn, ca):
        """
        Get user's groups in VO from VOMS server
        :param dn: DN of user's certificate
        :param ca: DN of the issuer of the certificate (CA)
        :return: list of groups
        """
        return self.voms.call_method("list-user-groups", dn, ca)

    def list_user_roles(self, dn, ca):
        return self.voms.call_method("list-user-roles", dn, ca)

    def get_user_nickname(self, dn, ca):
        attributes = self.list_user_attributes(dn, ca)
        return attributes.get("nickname")

    @staticmethod
    def get_identity_options(voms_admin_path="/usr/bin/voms-admin"):
        import imp
        voms_admin = imp.load_source("voms_admin", voms_admin_path)
        voms_admin.vlog = lambda msg: None
        voms_admin.setup_identity()

        return voms_admin.options

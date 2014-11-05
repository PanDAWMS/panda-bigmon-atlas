import os

from VOMSAdmin.VOMSCommands import VOMSAdminProxy


class VomsInterface:
    """
    Retrieving information from VOMS server.
    """

    def __init__(self, options):
        self.options = dict(options)
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
    def get_identity_options(voms_admin_path="/usr/bin/voms-admin", proxy_cert=None):
        import imp
        voms_admin = imp.load_source("voms_admin", voms_admin_path)
        voms_admin.vlog = lambda msg: None

        if proxy_cert:
            old_proxy = os.environ.get("X509_USER_PROXY")
            os.environ["X509_USER_PROXY"] = proxy_cert
            voms_admin.setup_identity()
            # Restore initial environment
            if old_proxy is None:
                del os.environ["X509_USER_PROXY"]
            else:
                os.environ["X509_USER_PROXY"] = old_proxy
        else:
            voms_admin.setup_identity()

        return voms_admin.options

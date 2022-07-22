from .local import CERT_PEM_PATH, CERT_KEY_PEM_PATH

JIRA_CONFIG = {
    'auth_url': 'https://its.cern.ch/jira/loginCern.jsp',
    'issue_url': 'https://its.cern.ch/jira/rest/api/2/issue/',
    'cert': CERT_PEM_PATH,
    'cert_key': CERT_KEY_PEM_PATH,
    'verify_ssl_certificates': False,
    'issue_template': {
        'fields': {
            'project': {
                'key': 'ATLPSTASKS'
            },
            'issuetype': {
                'name': 'Information Request'
            },
            'summary': "%s",
            'description': "%s"
        }
    },
    'sub_issue_template': {
        'fields': {
            'project': {
                'key': 'ATLPSTASKS'
            },
            'issuetype': {
                'name': 'Sub-task'
            },
            'summary': "%s",
            'description': "%s",
            'parent': {
                'key': "%s"
            }
        }
    },
    'issue_comment_template': {
        'body': "%s"
    },
    'issue_close_template': {
        'update': {
            'comment': [
                {'add': {'body': "%s"}}
            ]
        },
        'fields': {
            'resolution': {
                'name': 'None'
            }
        },
        'transition': {
            'id': '2'
        },
    }
}
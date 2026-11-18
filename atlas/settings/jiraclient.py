from .runtime import CERT_PEM_PATH, CERT_KEY_PEM_PATH, JIRA_KEY, JIRA_AUTH_URL, JIRA_ISSUE_URL

JIRA_CONFIG = {
    'jira_key': JIRA_KEY,
    'auth_url': JIRA_AUTH_URL,
    'issue_url': JIRA_ISSUE_URL,
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
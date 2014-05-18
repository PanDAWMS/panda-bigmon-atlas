""" 
    atlas.version
"""
import commands
import ConfigParser
import os


def get_version_base__release_type__provides():
    """
        get_version_base ... get version string base from the setup.cfg
    """
    config = ConfigParser.ConfigParser()
    config.read(os.path.dirname(os.path.realpath(__file__)) + '/setup.cfg')
    version_base = config.get("global", "version")
    release_type = config.get("global", "release_type")
    provides = config.get("bdist_rpm", "provides")
    return (version_base, release_type, provides,)


def get_svn_version():
    """
        get version of this svn commit
    """
    dir = os.path.dirname(os.path.realpath(__file__))
    ### get last revision ID, short version
    last_rev_id = ''
    try:
        last_rev_id = commands.getoutput(' svnversion atlas/ ')
    except:
        pass
    return str('.dev-r' + last_rev_id)


def get_version_provides():
#    __version__ = get_svn_version()
#    __provides__ = get_version_base__release_type__provides()
#    return (__version__, __provides__,)
    isStable = False
    version_base, release_type, provides = get_version_base__release_type__provides()
    if release_type == 'stable':
        isStable = True
    __version__ = version_base
    if not isStable:
        __version__ += get_svn_version()
    __provides__ = provides
    return (__version__, __provides__,)


__version__, __provides__ = get_version_provides()



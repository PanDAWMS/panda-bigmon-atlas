import threading

class SingletonException(Exception):
    pass

_stSingletons = set()
_lockForSingletons = threading.RLock()
_lockForSingletonCreation = threading.RLock()   # Ensure only one instance of each Singleton
                                                # class is created.  This is not bound to the
                                                # individual Singleton class since we need to
                                                # ensure that there is only one mutex for each
                                                # Singleton class, which would require having
                                                # a lock when setting up the Singleton class,
                                                # which is what this is anyway.  So, when any
                                                # Singleton is created, we lock this lock and
                                                # then we don't need to lock it again for that
                                                # class.

def _createSingletonInstance(cls, lstArgs, dctKwArgs):
    _lockForSingletonCreation.acquire()
    try:
        if cls._isInstantiated(): # some other thread got here first
            return

        instance = cls.__new__(cls)
        try:
            instance.__init__(*lstArgs, **dctKwArgs)
        except TypeError as e:
            if e.message.find('__init__() takes') != -1:
                raise SingletonException('If the singleton requires __init__ args, supply them on first call to getInstance().')
            else:
                raise
        cls.cInstance = instance
        _addSingleton(cls)
    finally:
        _lockForSingletonCreation.release()

def _addSingleton(cls):
    _lockForSingletons.acquire()
    try:
        assert cls not in _stSingletons
        _stSingletons.add(cls)
    finally:
        _lockForSingletons.release()

def _removeSingleton(cls):
    _lockForSingletons.acquire()
    try:
        if cls in _stSingletons:
            _stSingletons.remove(cls)
    finally:
        _lockForSingletons.release()

def forgetAllSingletons():
    '''This is useful in tests, since it is hard to know which singletons need to be cleared to make a test work.'''
    _lockForSingletons.acquire()
    try:
        for cls in _stSingletons.copy():
            cls._forgetClassInstanceReferenceForTesting()

        # Might have created some Singletons in the process of tearing down.
        # Try one more time - there should be a limit to this.
        iNumSingletons = len(_stSingletons)
        if len(_stSingletons) > 0:
            for cls in _stSingletons.copy():
                cls._forgetClassInstanceReferenceForTesting()
                iNumSingletons -= 1
                assert iNumSingletons == len(_stSingletons), 'Added a singleton while destroying ' + str(cls)
        assert len(_stSingletons) == 0, _stSingletons
    finally:
        _lockForSingletons.release()

class MetaSingleton(type):
    def __new__(metaclass, strName, tupBases, dct):
        if '__new__' in dct:
            raise SingletonException('Can not override __new__ in a Singleton')
        return super(MetaSingleton, metaclass).__new__(metaclass, strName, tupBases, dct)

    def __call__(cls, *lstArgs, **dictArgs):
        raise SingletonException('Singletons may only be instantiated through getInstance()')

class Singleton(object, metaclass=MetaSingleton):
    def getInstance(cls, *lstArgs, **dctKwArgs):
        """
        Call this to instantiate an instance or retrieve the existing instance.
        If the singleton requires args to be instantiated, include them the first
        time you call getInstance.
        """
        if cls._isInstantiated():
            if (lstArgs or dctKwArgs) and not hasattr(cls, 'ignoreSubsequent'):
                raise SingletonException('Singleton already instantiated, but getInstance() called with args.')
        else:
            _createSingletonInstance(cls, lstArgs, dctKwArgs)

        return cls.cInstance
    getInstance = classmethod(getInstance)

    def _isInstantiated(cls):
        # Don't use hasattr(cls, 'cInstance'), because that screws things up if there is a singleton that
        # extends another singleton.  hasattr looks in the base class if it doesn't find in subclass.
        return 'cInstance' in cls.__dict__
    _isInstantiated = classmethod(_isInstantiated)

    # This can be handy for public use also
    isInstantiated = _isInstantiated

    def _forgetClassInstanceReferenceForTesting(cls):
        """
        This is designed for convenience in testing -- sometimes you
        want to get rid of a singleton during test code to see what
        happens when you call getInstance() under a new situation.

        To really delete the object, all external references to it
        also need to be deleted.
        """
        try:
            if hasattr(cls.cInstance, '_prepareToForgetSingleton'):
                # tell instance to release anything it might be holding onto.
                cls.cInstance._prepareToForgetSingleton()
            del cls.cInstance
            _removeSingleton(cls)
        except AttributeError:
            # run up the chain of base classes until we find the one that has the instance
            # and then delete it there
            for baseClass in cls.__bases__:
                if issubclass(baseClass, Singleton):
                    baseClass._forgetClassInstanceReferenceForTesting()
    _forgetClassInstanceReferenceForTesting = classmethod(_forgetClassInstanceReferenceForTesting)


def form_request_log(reqid,request,message):
    user_name = ''
    try:
        user_name = request.user.username
    except:
        pass
    return 'request:%s user:%s %s'%(str(reqid),user_name,message)
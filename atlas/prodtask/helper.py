


def form_request_log(reqid,request,message):
    user_name = ''
    try:
        user_name = request.user.username
    except:
        pass
    return 'request:%s user:%s %s'%(str(reqid),user_name,message)
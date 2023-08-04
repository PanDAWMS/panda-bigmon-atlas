==============================================================
Perform fake authentication as a specified user
==============================================================

Prequsities
+++++++++++

User to authenticate as should exist in the database (table 'auth_user').


Configuration
+++++++++++++


settings/config.py
..................

* Add 'atlas.auth.fake.backends.LoginAsBackend' to AUTHENTICATION_BACKENDS 
before all other backends.

* Set FAKE_LOGIN_AS_USER = 'username' , pointing to the desired username existing
in the database


urls.py
.......

* Add (or change) the following lines for your login and logout urls:
re_path(r'^login/$', 'auth.fake.views.login', name='login'),

re_path(r'^logout/$', 'auth.fake.views.logout', name='logout'),

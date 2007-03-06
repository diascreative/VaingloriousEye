The Vainglorious Eye
====================

.. contents::

Introduction and Status
-----------------------

The Vainglorious Eye is a WSGI middleware to track requests in a
website.  It's similar in scope to things like `Analog
<http://www.analog.cx/>`_, except that it does the tracking live (not
through log processing), and offers the data through RESTful services.

Usage
-----

Put the middleware in place, either using
``vaineye.statustracker.StatusTracker`` or with the Paste Deploy
configuration like::

    [filter-app:status]
    use = egg:VaingloriousEye
    next = real_app

License
-------

The Vainglorious Eye is distributed under the `MIT license
<http://www.opensource.org/licenses/mit-license.php>`_.

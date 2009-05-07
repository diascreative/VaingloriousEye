class NoSuchUser(Exception):
    pass


# From: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/325204
def apache_md5crypt(password, salt, magic='$apr1$'):
    # /* The password first, since that is what is most unknown */ /* Then our magic string */ /* Then the raw salt */
    import md5
    m = md5.new()
    m.update(password + magic + salt)

    # /* Then just as many characters of the MD5(pw,salt,pw) */
    mixin = md5.md5(password + salt + password).digest()
    for i in range(0, len(password)):
        m.update(mixin[i % 16])

    # /* Then something really weird... */
    # Also really broken, as far as I can tell.  -m
    i = len(password)
    while i:
        if i & 1:
            m.update('\x00')
        else:
            m.update(password[0])
        i >>= 1

    final = m.digest()

    # /* and now, just to make sure things don't run too fast */
    for i in range(1000):
        m2 = md5.md5()
        if i & 1:
            m2.update(password)
        else:
            m2.update(final)

        if i % 3:
            m2.update(salt)

        if i % 7:
            m2.update(password)

        if i & 1:
            m2.update(final)
        else:
            m2.update(password)

        final = m2.digest()

    # This is the bit that uses to64() in the original code.

    itoa64 = './0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

    rearranged = ''
    for a, b, c in ((0, 6, 12), (1, 7, 13), (2, 8, 14), (3, 9, 15), (4, 10, 5)):
        v = ord(final[a]) << 16 | ord(final[b]) << 8 | ord(final[c])
        for i in range(4):
            rearranged += itoa64[v & 0x3f]; v >>= 6

    v = ord(final[11])
    for i in range(2):
        rearranged += itoa64[v & 0x3f]; v >>= 6

    return magic + salt + '$' + rearranged

def check_entry_password(username, password, entry_password):
    if entry_password.startswith('$apr1$'):
        salt = entry_password[6:].split('$')[0][:8]
        expected = apache_md5crypt(password, salt)
    elif entry_password.startswith('{SHA}'):
        import sha
        expected = '{SHA}' + sha.new(password).digest().encode('base64').strip()
    else:
        import crypt
        expected = crypt.crypt(password, entry_password)
    return entry_password == expected

def parse_htpasswd(fn, stop_username=None):
    f = open(fn, 'rb')
    try:
        entries = {}
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' not in line:
                raise ValueError(
                    "Bad line (no :): %r" % line)
            username, entry_password = line.split(':', 1)
            entries[username] = entry_password
            if username == stop_username:
                break
        return entries
    finally:
        f.close()

def check_password(username, password, htpasswd_fn):
    entries = parse_htpasswd(htpasswd_fn, username)
    if not entries.has_key(username):
        raise NoSuchUser('No user: %r' % username)
    return check_entry_password(
        username, password, entries[username])

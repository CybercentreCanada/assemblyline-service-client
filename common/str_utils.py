import re


# According to wikipedia, RFC 3629 restricted UTF-8 to end at U+10FFFF.
# This removed the 6, 5 and (irritatingly) half of the 4 byte sequences.
#
# The start byte for 2-byte sequences should be a value between 0xc0 and
# 0xdf but the values 0xc0 and 0xc1 are invalid as they could only be
# the result of an overlong encoding of basic ASCII characters. There
# are similar restrictions on the valid values for 3 and 4-byte sequences.
_valid_utf8 = re.compile(r"""((?:
    [\x09\x0a\x20-\x7e]|             # 1-byte (ASCII excluding control chars).
    [\xc2-\xdf][\x80-\xbf]|          # 2-bytes (excluding overlong sequences).
    [\xe0][\xa0-\xbf][\x80-\xbf]|    # 3-bytes (excluding overlong sequences).

    [\xe1-\xec][\x80-\xbf]{2}|       # 3-bytes.
    [\xed][\x80-\x9f][\x80-\xbf]|    # 3-bytes (up to invalid code points).
    [\xee-\xef][\x80-\xbf]{2}|       # 3-bytes (after invalid code points).

    [\xf0][\x90-\xbf][\x80-\xbf]{2}| # 4-bytes (excluding overlong sequences).
    [\xf1-\xf3][\x80-\xbf]{3}|       # 4-bytes.
    [\xf4][\x80-\x8f][\x80-\xbf]{2}  # 4-bytes (up to U+10FFFF).
    )+)""", re.VERBOSE)


def _escape(t, reversible=True):
    if t[0] % 2:
        return t[1].replace('\\', '\\\\') if reversible else t[1]
    else:
        return ''.join(('\\x%02x' % ord(x)) for x in t[1])


def escape_str(s, reversible=True, force_str=False):
    if isinstance(s, bytes):
        return escape_str_strict(s.decode('utf8'), reversible)
    elif not isinstance(s, str):
        if force_str:
            return str(s)
        return s

    return escape_str_strict(s, reversible)


# Returns a string (str) with only valid UTF-8 byte sequences.
def escape_str_strict(s, reversible=True):
    return ''.join([_escape(t, reversible)
                    for t in enumerate(_valid_utf8.split(s))])


def safe_str(s, force_str=False):
    return escape_str(s, reversible=False, force_str=force_str)


# This method not really necessary. More to stop people from rolling their own.
def unescape_str(s):
    return s.decode('string_escape')


class NamedConstants(object):

    def __init__(self, name, string_value_list):
        self._name = name
        self._value_map = dict(string_value_list)
        self._reverse_map = dict([(s[1], s[0]) for s in string_value_list])

        # we also import the list as attributes so things like
        # tab completion and introspection still work.
        for s, v in self._value_map.items():
            setattr(self, s, v)

    def name_for_value(self, v):
        return self._reverse_map[v]

    def contains_value(self, v):
        return v in self._reverse_map

    def __getitem__(self, s):
        return self._value_map[s]

    def __getattr__(self, s):
        # We implement our own getattr mainly to provide the better exception.
        return self._value_map[s]


class StringTable(object):

    def __init__(self, name, string_value_list):
        self._name = name
        self._value_map = dict(string_value_list)
        self._reverse_map = dict([(s[1], s[0]) for s in string_value_list])

        # we also import the list as attributes so things like
        # tab completion and introspection still work.
        for s in self._value_map.keys():
            setattr(self, s, s)

    def name_for_value(self, v):
        return self._reverse_map[v]

    def contains_string(self, s):
        return s in self._reverse_map

    def contains_value(self, v):
        return v in self._value_map

    def __getitem__(self, s):
        if s in self._value_map:
            return s
        raise AttributeError("Invalid value for %s (%s)" % (self._name, s))

    def __getattr__(self, s):
        # We implement our own getattr mainly to provide the better exception.
        if s in self._value_map:
            return s
        raise AttributeError("Invalid value for %s (%s)" % (self._name, s))

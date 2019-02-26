import sys

from types import ModuleType


class MockAssemblylineAlService(ModuleType):
    def __init__(self):
        super(MockAssemblylineAlService, self).__init__('assemblyline.al.service')
        import common.base
        self.base = common.base
        self.base.__name__ = 'assemblyline.al.service.base'


class MockAssemblylineAlCommon(ModuleType):
    def __init__(self):
        super(MockAssemblylineAlCommon, self).__init__('assemblyline.al.common')
        import common.result
        self.result = common.result
        self.result.__name__ = 'assemblyline.al.common.result'


class MockAssemblylineCommon(ModuleType):
    def __init__(self):
        super(MockAssemblylineCommon, self).__init__('assemblyline.al')
        from assemblyline.common import str_utils
        self.charset = str_utils
        self.charset.__name__ = 'assemblyline.common.charset'
        from common import mock_forge
        self.forge = mock_forge
        self.forge.__name__ = 'assemblyline.common.forge'


class MockAssemblylineAl(ModuleType):
    def __init__(self):
        super(MockAssemblylineAl, self).__init__('assemblyline.al')
        self.common = MockAssemblylineAlCommon()
        self.common.__name__ = 'assemblyline.al.common'
        self.service = MockAssemblylineAlService()
        self.service.__name__ = 'assemblyline.al.service'


class MockAssemblyline(ModuleType):
    def __init__(self):
        super(MockAssemblyline, self).__init__('assemblyline')
        self.al = MockAssemblylineAl()
        self.al.__name__ = 'assemblyline.al'
        import assemblyline.common as common
        from assemblyline.common import str_utils
        common.charset = str_utils
        self.common = common
        from common import mock_forge
        common.forge = mock_forge
        common.forge.__name__ = 'assemblyline.common.forge'


class MockAssemblyline2AlCommon(ModuleType):
    def __init__(self):
        super(MockAssemblyline2AlCommon, self).__init__('assemblyline.al.common')
        from assemblyline.common import heuristics
        self.heuristics = heuristics
        self.heuristics.__name__ = 'assemblyline.al.common.heuristics'


class MockAssemblyline2Al(ModuleType):
    def __init__(self):
        super(MockAssemblyline2Al, self).__init__('assemblyline.al')
        self.common = MockAssemblyline2AlCommon()
        self.common.__name__ = 'assemblyline.al.common'


class MockAssemblyline2(ModuleType):
    def __init__(self):
        super(MockAssemblyline2, self).__init__('assemblyline')
        self.al = MockAssemblyline2Al()
        self.al.__name__ = 'assemblyline.al'


def modules1():
    mock_al = MockAssemblyline()
    sys.modules['assemblyline'] = mock_al
    sys.modules['assemblyline.al'] = mock_al.al
    sys.modules['assemblyline.al.common'] = mock_al.al.common
    sys.modules['assemblyline.al.common.result'] = mock_al.al.common.result
    sys.modules['assemblyline.al.service'] = mock_al.al.service
    sys.modules['assemblyline.al.service.base'] = mock_al.al.service.base
    sys.modules['assemblyline.common'] = mock_al.common
    sys.modules['assemblyline.common.charset'] = mock_al.common.charset
    sys.modules['assemblyline.common.forge'] = mock_al.common.forge


def modules2():
    mock_al = MockAssemblyline2()
    sys.modules['assemblyline'] = mock_al
    sys.modules['assemblyline.al'] = mock_al.al
    sys.modules['assemblyline.al.common'] = mock_al.al.common
    sys.modules['assemblyline.al.common.heuristics'] = mock_al.al.common.heuristics

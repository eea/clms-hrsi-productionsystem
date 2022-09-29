from _io import BytesIO
from xml.etree import ElementTree


class XmlUtil(object):
    '''XML utility functions'''

    @staticmethod
    def namespace(content):
        '''Return the namespaces of an XML document.'''
        
        # Get namespaces
        ns = dict([
            node for _, node in ElementTree.iterparse(
                BytesIO(content), events=['start-ns'])])
        
        # Add default namespace with key 'xmlns'
        try:
            ns['xmlns'] = ns['']
        except Exception:
            pass
        
        return ns
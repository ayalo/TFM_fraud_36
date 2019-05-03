import string
import re
from tld import get_tld
from tld.utils import update_tld_names


WWW = re.compile('^www\.')

"""
def parse_domain(domain) :
    if not domain:
        return domain
    domain = domain.lower() # force lowercase to use binary collate in the db
    # app ids come clean
    if domain.isdigit():
        return domain
    if domain.startswith('com.'):
        return domain.split('&',1)[0]
    print( "domain before join" + domain)
    domain = ''.join(filter(string.printable.__contains__, domain)) # remove non printable characters
    print( "domain after join" + domain )
    domain_cleaned = WWW.subn('',domain.replace('"','').strip().split('//',1)[-1].split('/',1)[0],count=1)[0]
    print ("domain_cleaned"+domain_cleaned)
    # return cleaned top level domain or discard
    try:
        print ("salida try"+ get_tld("http://" + domain_cleaned ))
        return get_tld("http://" + domain_cleaned )
    except:
        return domain_cleaned
"""
def parse_domain(domain) :
    if not domain:
        return domain
def main(d):
    print ("cleaned_domain : "+parse_domain(d))


#if __name__ == '__main__':
#    if len(sys.argv) > 1:
 #       main(sys.argv[1])
if __name__ == '__main__':
    #salida=parse_domain("com.https://www.'\x00\x11Hello'flash.com/light.brightest/.BEACON'\x00\x11Hello'conQUESO437364525289.torch.comr")
    #print ("salida : "+salida)

    print ("prueba get_tld")
    print (""+get_tld( "http://www.google.co.uk" ))
    print
    print (""+get_tld( "https://zap.co.it" ))
    print
    print (""+get_tld( "http://google.com" ))
    print
    print (""+get_tld( "https://mail.google.com" ))
    print
    print (""+get_tld( "http://mail.google.co.uk" ))
    print
    print (""+get_tld( "http://www.google.co.uk" ))
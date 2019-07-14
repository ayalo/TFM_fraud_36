import sys
import re
import string
from tld import get_fld


def domain_cleaner(domain):
    """
    We left only lower case, clean certain characters and only printable ones
    We get only the main domain, or the first part til the first / begging from the left
    We substract '.com' from the begging and 'https://' 'http://' 'www.'
    :arg domain : String
    :return: cleaned_domain
    """
    domain = domain.lower()
    dirty_string = re.sub( "www.|.www|https:\/\/|http:\/\/|\"|\'", "", domain )  ##.lower()
    if domain.isdigit():
        return domain

    clean_string = ''.join( filter( lambda x: x in string.printable, dirty_string ) )  ## only printable characters
    ##print ("domain_cleaner -- domain_cleaner -- clean_string : "+clean_string)

    dirty_string = clean_string.split( '/', 1 )[0]  ## only the main_domain
    ##print ("domain_cleaner -- return : "+ dirty_string)

    cleaned_domain = delete_ini( dirty_string, "com." )
    ##print( "domain_cleaner -- dirty_string : " + cleaned_domain )

    # print( get_fld( "http://www.google.co.uk" ) )
    # cleaned_root_domain = get_fld( "http://" + cleaned_domain )
    # print( "cleaned_domain : "+cleaned_root_domain)

    try:
        ##print ("get_fld http -- " )
        # get_fld("http://" + cleaned_domain)
        return get_fld( "http://" + cleaned_domain )
    except:
        ##print ("get_fld except --" +cleaned_domain)
        return cleaned_domain


'''
def filter_string(s):  ##creo que no se usa, idem que is_string en df_utils
    # return isinstance( s, basestring )
    return type( s ) is str
'''


# funcion para eliminar el inicio de un string recursivamente, por ejemplo si comienza por com.com.com.google.es, nos quedamos con google.es
def delete_ini(text, subString):
    ##print ("delete_ini -- subString : "+ subString)
    ##print ("delete_ini -- text : "+ text)
    return delete_ini( text[len( subString ):], subString ) if text[:len( subString )] == subString else text


def ip_cleaner(ip):
    """
    To verify that the IP has this format  : X.X.X.X
    :arg ip : String
    :return: cleaned_ip / "Format not valid"
    """
    if valid_ip( ip ): return ip

    return "Format not valid"
    # return cleaned_ip


def valid_ip(ip):
    """
    To verify that the IP has this format  : X.X.X.X
    :param ip:
    :return: Boolean
    """
    if ip is None: return False
    part = ip.split( '.' )
    if len( part ) != 4: return False
    try:
        return all( 0 <= int( p ) < 256 for p in part )
    except ValueError:
        return False

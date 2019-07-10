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
        domain=domain.lower()
        dirty_string= re.sub("www.|.www|https:\/\/|http:\/\/|\"|\'", "", domain)##.lower()
        if domain.isdigit():
            return domain

        clean_string =  ''.join(filter(lambda x: x in string.printable, dirty_string)) ## only printable characters
        ##print ("domain_cleaner -- domain_cleaner -- clean_string : "+clean_string)

        dirty_string=clean_string.split('/',1)[0]## only the main_domain
        ##print ("domain_cleaner -- return : "+ dirty_string)


        cleaned_domain = delete_ini( dirty_string, "com." )
        ##print( "domain_cleaner -- dirty_string : " + cleaned_domain )

        #print( get_fld( "http://www.google.co.uk" ) )
        #cleaned_root_domain = get_fld( "http://" + cleaned_domain )
        #print( "cleaned_domain : "+cleaned_root_domain)

        try:
            ##print ("get_fld http -- " )
            #get_fld("http://" + cleaned_domain)
            return get_fld("http://" + cleaned_domain )
        except:
            ##print ("get_fld except --" +cleaned_domain)
            return cleaned_domain



def filter_string(s):
   # return isinstance( s, basestring )
    return type(s) is str

# funcion para eliminar el inicio de un string recursivamente, por ejemplo si comienza por com.com.com.google.es, nos quedamos con google.es
def delete_ini(text, subString):
    ##print ("delete_ini -- subString : "+ subString)
    ##print ("delete_ini -- text : "+ text)
    return delete_ini( text[len( subString ):], subString ) if text[:len( subString )] == subString else text

def main(d):
    print ("cleaned_domain : "+domain_cleaner(d))

#if __name__ == '__main__':
#    if len(sys.argv) > 1:
#        main(sys.argv[1])

if __name__ == '__main__':
    salida=domain_cleaner("com.com.otracoasa.com.https://www.'\x00\x11Hello'flash.com/light.brightest/.BEACON'\x00\x11Hello'conQUESO437364525289.torch.comr")
    print ("salida : "+salida)
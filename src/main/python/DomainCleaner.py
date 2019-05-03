import sys
import re
import string

def domain_cleaner(domain):
        """
        We left only lower case, clean certain characters and only printable ones
        We get only the main domain, or the first part til the first / begging from the left
        We substract '.com' from the begging and 'https://' 'http://' 'www.'
        :arg domain : String
        :return: cleaned_domain
        """

        dirty_string= re.sub("www.|.www|https:\/\/|http:\/\/|\"|\'", "", domain).lower()
        #if domain.isdigit():
        #    return domain
        #if dirty_string.startswith(("com.", ".", ".com")):
        #   dirty_string.split('&', 1)[0]
        dirty_string=delete_ini(dirty_string,"com.")
        print ("domain_cleaner -- dirty_string : "+dirty_string)

        clean_string =  ''.join(filter(lambda x: x in string.printable, dirty_string)) ## only printable characters
        #print ("domain_cleaner -- domain_cleaner -- clean_string : "+clean_string)

        cleaned_domain=clean_string.split('/',1)[0]## only the main_domain
        print ("domain_cleaner -- return : "+ cleaned_domain)

        return cleaned_domain

def delete_ini(text, subString):
    print ("delete_ini -- subString : "+ subString)
    print ("delete_ini -- text : "+ text)
    return delete_ini( text[len( subString ):], subString ) if text[:len( subString )] == subString else text

def main(d):
    print ("cleaned_domain : "+domain_cleaner(d))

#if __name__ == '__main__':
#    if len(sys.argv) > 1:
#        main(sys.argv[1])

if __name__ == '__main__':
    salida=domain_cleaner("com.com.otracoasa.com.https://www.'\x00\x11Hello'flash.com/light.brightest/.BEACON'\x00\x11Hello'conQUESO437364525289.torch.comr")
    print ("salida : "+salida)
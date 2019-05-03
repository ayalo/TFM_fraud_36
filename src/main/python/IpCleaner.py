import sys
import re
import string

def ip_cleaner(ip):
        """
        To verify that the IP has this format  : X.X.X.X
        :arg ip : String
        :return: cleaned_ip
        """
        if valid_ip(ip): return ip

        return "Format not valid: " +ip
        # return cleaned_ip

def valid_ip(ip):
    part = ip.split( '.' )
    if len( part ) != 4: return False
    try:
        return all( 0 <= int( p ) < 256 for p in part )
    except ValueError:
        return False

def main(i):
    print ("cleaned_domain : "+ip_cleaner(i))

#if __name__ == '__main__':
#    if len(sys.argv) > 1:
#        main(sys.argv[1])

if __name__ == '__main__':
    salida=ip_cleaner("178.23.5")
    print ("salida : "+salida)
    salida = ip_cleaner( "2001.DB9:1" )
    print( "salida : " + salida )
    salida = ip_cleaner( "2001.DB9:1.23.5" )
    print( "salida : " + salida )
    salida = ip_cleaner( "10.34.76.23" )
    print( "salida : " + salida )
    salida = ip_cleaner( "178.23.5.34.7" )
    print( "salida : " + salida )
    salida = ip_cleaner( "holacaracola" )
    print( "salida : " + salida )
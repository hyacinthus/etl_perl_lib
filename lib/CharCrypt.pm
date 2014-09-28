#!/usr/bin/perl
#########################################################################################################
#Function:              字符加密解密模块
#Author:                WuGang
#Date Time:
#History:
#Copyright              2011 VanceInfo All Rights Reserved.
#########################################################################################################
package CharCrypt;
use strict;
use Crypt::RC4;

my $key = '12345678';

#rc4解密
sub rc4Decrypt{
    my $encode = $_[0];                     #密文
    if(!defined $encode or length $encode == 0){return '';}
    my $asciiStr;
    for(my $i = 0; $i< length $encode;$i=$i+2){
        $asciiStr .= chr(hex(substr($encode,$i,2)));
    }
    my $rc4    = Crypt::RC4->new($key);      #rc4解密
    my $encrypted = $rc4->RC4($asciiStr);
    return $encrypted;
}

#rc4加密
sub rc4Encrypt {
    my $plaintext = $_[0];                  #明文
    my $rc4       = Crypt::RC4->new($key);
    my $encrypted = $rc4->RC4($plaintext);
    my $ascii = '';
    #取密文ASCII值并将其转化为十六进制形式
    for ( my $i = 0 ; $i < length $encrypted ; $i++ ) {
        my $hex = uc( sprintf( '%x', ord( substr( $encrypted, $i, 1 ) ) ) );
        if ( length $hex < 2 ) { $hex = '0'.$hex; }
        $ascii .= $hex;
    }
    return $ascii;
}

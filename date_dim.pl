use strict;
use Date::Calc qw(:all);

my $tmp_string;

#begin date
my $year = 2012;
my $month = 12;
my $day = 1;

#end date
my $endyear = 2012;
my $endmonth = 12;
my $endday = 31;

#season definition
my %season=(3=>'一',6=>'二',9=>'三',12=>'四');

#create date dimension
while (1)
{
    #daily
    $tmp_string = "${year}年${month}月${day}日";
    print $tmp_string;
    #monthly
    print Days_in_Month($year,$month);
    if ($day == Days_in_Month($year,$month))
    {
        $tmp_string = "${year}年${month}月";
        print $tmp_string;
    }
    #quartly
    if ( $day == Days_in_Month($year,$month )
            && exists ${season}{$month})
    {
        $tmp_string = "${year}年第${season{$month}}季度";
        print $tmp_string;
    }
    #yearly
    if ( $month == 12 && $day == 31 )
    {
        $tmp_string = "${year}年";
        print $tmp_string;
    }

    #add a day
    if ( $year == $endyear && $month == $endmonth && $day == $endday)
    {
        last;
    }
    else
    {
        ($year,$month,$day) = Add_Delta_Days($year,$month,$day,1);
    }
}

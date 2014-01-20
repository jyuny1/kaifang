#use strict;
use DBI;

my $dbh
    = DBI->connect( "dbi:SQLite:dbname=2k.db", "", "",
    { RaiseError => 1, AutoCommit => 0 } )
    or die $DBI::errstr;

my %mobileData = readAreaCode();

# my $i              = 1;
my $totalRowNumber = 20050144;
my $threshole      = 1000000;

# my ($name,      $cardno,    $descriot,  $ctftp,     $ctfid,     $gender,
#     $birthday,  $address,   $zip,       $dirty,     $district1, $district2,
#     $district3, $district4, $district5, $district6, $firstName, $lastName,
#     $duty,      $mobile,    $tel,       $fax,       $email
# ) = "null";

#turncateTable();

for ( my $i = 1; $i <= $totalRowNumber; $i += $threshole ) {

    # print "reading data into memory...";
    my $is_power = 0;
    my $selectStatement = sprintf(
        "select * from cdsgus where rowid between %d and %d",
        $i,
        ( ( $i + $threshole ) > $totalRowNumber )
        ? $totalRowNumber
        : $i + $threshole -1
    
    print "$selectStatement\n";

    my $sth = $dbh->prepare($selectStatement);
    $sth->execute();
    my $all = $sth->fetchall_arrayref();
    
    my $i = 1;

    foreach my $row (@$all) {
        my ($name,      $cardno,    $descriot,  $ctftp,     $ctfid,
            $gender,    $birthday,  $address,   $zip,       $dirty,
            $district1, $district2, $district3, $district4, $district5,
            $district6, $firstName, $lastName,  $duty,      $mobile,
            $tel,       $fax,       $email
            )
            = (
            @$row[0],  @$row[1],  @$row[2],  @$row[3],  @$row[4],
            @$row[5],  @$row[6],  @$row[7],  @$row[8],  @$row[9],
            @$row[10], @$row[11], @$row[12], @$row[13], @$row[14],
            @$row[15], @$row[16], @$row[17], @$row[18], @$row[19],
            @$row[20], @$row[21], @$row[22], @$row[23], @$row[24],
            @$row[25], @$row[26], @$row[27]
            );

        $name =~ tr /"/\s/;
        $cardno =~ tr /"/\s/;
        $ctftp =~ tr /"/\s/;
        $ctfid =~ tr /"/\s/;
        $gender =~ tr /"/\s/;
        $birthday =~ tr /"/\s/;
        $address =~ tr /"/\s/;

        my $birthYear  = substr( $birthday, 0, 4 );
        my $birthMonth = substr( $birthday, 4, 2 );
        my $mobileID   = substr( $mobile,   0, 7 );
        my $customer   = idCheck($ctfid);

        if ( $email = $email =~ m/(.*)\@(.*)/ ? $2 : "" ) {
            $email = lc $2;
        }
        else {
            $email = "null";
        }

        my $sql = sprintf(
            "INSERT INTO cdsgus2 (name, idNumber, gender, birthYear, birthMonth, address, mobile, idAge, idGender, idBirthYear, idBirthMonth, idBirthProvince, idBirthCounty, idBirthCity, mobileProvince, mobileCounty, mobileType, idVarified, email) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")",
            $name,
            uc $ctfid,
            $gender,
            $birthYear,
            $birthMonth,
            $address,
            $mobile,
            $customer->{age},
            $customer->{gender},
            $customer->{birthYear},
            $customer->{birthMonth},
            $customer->{birthProvince},
            $customer->{birthCounty},
            $customer->{birthCity},
            exists $mobileData{$mobileID}->{province}
            ? $mobileData{$mobileID}->{province}
            : "null",
            exists $mobileData{$mobileID}->{county}
            ? $mobileData{$mobileID}->{county}
            : "null",
            exists $mobileData{$mobileID}->{type}
            ? $mobileData{$mobileID}->{type}
            : "null",
            $customer->{valid},
            $email
        );
        
        #print "$sql\n";

        $dbh->do($sql);

        if ( $i % ($threshole / 10 ) == 0 || $i % $totalRowNumber == 0) {
            $dbh->commit();
            print "$i: commit\n";
        }
        $i++;
    }
}

$sth->finish();
$dbh->disconnect();

#=================================================================

sub turncateTable {
    my $sql1 = "delete from 'cdsgus2';";
    my $sql2 = "select * from sqlite_sequence;";
    my $sql3 = "update sqlite_sequence set seq=0 where name='cdsgus2';";

    $dbh->do($sql1);
    $dbh->do($sql2);
    $dbh->do($sql3);

    $dbh->commit();

    print "table flashed\n";
}

sub readAreaCode {
    my $dbh = DBI->connect(
        "dbi:SQLite:dbname=mobile.sqlite",
        "", "", { RaiseError => 1 },
    ) or die $DBI::errstr;

    my $sth = $dbh->prepare("SELECT * FROM Dm_Mobile");
    $sth->execute();

    print "reading area data into memory...";
    my $all = $sth->fetchall_arrayref();
    print "done\n";

    my %hash_table = ();

    foreach my $row (@$all) {
        my $mobileInfo = {
            type     => shift,
            province => shift,
            county   => shift
        };

        my ( $id, $mobileNumber, $mobileArea, $mobileType, $areaCode,
            $postCode )
            = @$row;

        ( $mobileInfo->{province}, $mobileInfo->{county} )
            = split( / /, $mobileArea );
        $mobileInfo->{type} = $mobileType;
        $hash_table{$mobileNumber} = $mobileInfo;

        #print $hash_table{$mobileNumber}->{province};
    }

# print everything in hash table foreach ( sort keys %hash_table ) {
#     print
#         "$_ : $hash_table{$_}->{province}:$hash_table{$_}->{county}:$hash_table{$_}->{type}\n";
# }

# get resault
#     print
#     "$hash_table{'1330000'}->{province}:$hash_table{'1330000'}->{county}:$hash_table{'1330000'}->{type}\n";

    $sth->finish();
    $dbh->disconnect();

    return %hash_table;
}

sub idCheck {
    my %provinceCode = (
        "11" => "京",
        "12" => "津",
        "13" => "冀",
        "14" => "晋",
        "15" => "蒙",

        "21" => "辽",
        "22" => "吉",
        "23" => "黑",

        "31" => "沪",
        "32" => "苏",
        "33" => "浙",
        "34" => "皖",
        "35" => "闽",
        "36" => "赣",
        "37" => "鲁",

        "41" => "豫",
        "42" => "鄂",
        "43" => "湘",
        "44" => "粤",
        "45" => "桂",
        "46" => "琼",

        "50" => "渝",
        "51" => "川",
        "52" => "贵",
        "53" => "云",
        "54" => "藏",

        "61" => "陕",
        "62" => "甘",
        "63" => "青",
        "64" => "宁",
        "65" => "新",

        "71" => "台",
        "81" => "港",
        "82" => "澳"
    );

    # my %modeRelationship = (
    #     0  => 1,
    #     1  => 0,
    #     2  => "x",
    #     3  => 9,
    #     4  => 8,
    #     5  => 7,
    #     6  => 6,
    #     7  => 5,
    #     8  => 4,
    #     9  => 3,
    #     10 => 2
    # );

    # my $idNumber = $_[0];
    my $idNumber = lc $_[0];
    my @constant = qw{7 9 10 5 8 4 2 1 6 3 7 9 10 5 8 4 2};
    my @idNumberArray;
    my $idSum = 0;

    my $customer = {
        id            => "null",
        age           => "null",
        birthYear     => "null",
        birthMonth    => "null",
        birthProvince => "null",
        birthCounty   => "null",
        birthCity     => "null",
        gender        => "null",
        validBits     => "",
        valid         => 0
    };
    $customer->{id} = $idNumber;

    if ( $idNumber
        =~ m/(\d{2})(\d{2})(\d{2})(\d{4})(\d{2})(\d{2})(\d{2})(\d)(\d|x)/ )
    {
        $customer->{birthProvince} = $provinceCode{$1};
        $customer->{birthCounty}   = $2;
        $customer->{birthCity}     = $3;
        $customer->{birthYear}     = $4;
        $customer->{age}           = ( (localtime)[5] + 1900 ) - $4;
        $customer->{birthMonth}    = $5;
        $customer->{gender}        = $8 % 2 == 0 ? "F" : "M";
        $customer->{validBits}     = $9;

        # print "\n========\n$9\n=======\n";

        # my $i = 0;
        # foreach (@constant) {
        #     $tmp = substr( $idNumber, $i++, 1 );
        #     $idSum += ( $tmp * $_ );

        #     # print "$i $_*$tmp = $idSum\n";
        # }

        # my $check = $modeRelationship{ $idSum % 11 };

        # if ( $customer->{validBits} eq 'x' ) {
        #     $customer->{valid} = $check == 10 ? 1 : 0;
        # }
        # else {
        #     $customer->{valid} = $check == $customer->{validBits} ? 1 : 0;
        # }
    }
    elsif ( $idNumber
        =~ m/(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d)(\d)(\d)/ )
    {
        $customer->{birthProvince} = $provinceCode{$1};
        $customer->{birthCounty}   = $2;
        $customer->{birthCity}     = $3;
        $customer->{birthYear}     = $4 + 1900;
        $customer->{age}           = ( (localtime)[5] ) - $4;
        $customer->{birthMonth}    = $5;
        $customer->{gender}        = $9 % 2 == 0 ? "F" : "M";
    }

    # else {
    #     print "ID: $idNumber not match\n";
    # }

    # print "customer id: ",   $customer->{id},            "\n";
    # print "birth year: ",    $customer->{birthYear},     "\n";
    # print "birth month: ",   $customer->{birthMonth},    "\n";
    # print "birthProvince: ", $customer->{birthProvince}, "\n";
    # print "birthCity:",      $customer->{birthCity},     "\n";
    # print "birthCounty:",    $customer->{birthCounty},   "\n";
    # print "gender: ",        $customer->{gender},        "\n";

    # print "valid: ", $customer->{valid},     "\n";
    # print "valid bits: ", $customer->{validBits}, "\n";
    # print "id sum: $idSum\n";
    return $customer;
}

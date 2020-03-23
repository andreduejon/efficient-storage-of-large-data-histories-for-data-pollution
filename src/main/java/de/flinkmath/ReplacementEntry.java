package de.flinkmath;

import java.time.LocalDateTime;

public class ReplacementEntry {
    public int id;
    public String nc_id;
    public LocalDateTime timestamp;
    public String county_id;
    public String county_desc;
    public String last_name;
    public String first_name;
    public String midl_name;
    public String house_num;
    public String street_dir;
    public String street_name;
    public String res_city_desc;
    public String state_cd;
    public String zip_code;
    public String area_cd;
    public String phone_num;
    public String race_cd;
    public String race_desc;
    public String ethnic_cd;
    public String ethnic_desc;
    public String party_cd;
    public String party_desc;
    public String sex_code;
    public String sex;
    public String age;
    public String age_group;
    public String name_prefix;
    public String name_suffix;
    public String half_code;
    public String street_type;
    public String street_suffix;
    public String unit_designator;
    public String unit_num;
    public String mail_addr1;
    public String mail_addr2;
    public String mail_addr3;
    public String mail_addr4;
    public String mail_city;
    public String mail_state;
    public String mail_zipcode;

    public ReplacementEntry(int id, String nc_id, LocalDateTime timestamp, String county_id, String county_desc, String last_name, String first_name, String midl_name, String house_num, String street_dir, String street_name, String res_city_desc, String state_cd, String zip_code, String area_cd, String phone_num, String race_cd, String race_desc, String ethnic_cd, String ethnic_desc, String party_cd, String party_desc, String sex_code, String sex, String age, String age_group, String name_prefix, String name_suffix, String half_code, String street_type, String street_suffix, String unit_designator, String unit_num, String mail_addr1, String mail_addr2, String mail_addr3, String mail_addr4, String mail_city, String mail_state, String mail_zipcode) {
        this.id = id;
        this.nc_id = nc_id;
        this.timestamp = timestamp;
        this.county_id = county_id;
        this.county_desc = county_desc;
        this.last_name = last_name;
        this.first_name = first_name;
        this.midl_name = midl_name;
        this.house_num = house_num;
        this.street_dir = street_dir;
        this.street_name = street_name;
        this.res_city_desc = res_city_desc;
        this.state_cd = state_cd;
        this.zip_code = zip_code;
        this.area_cd = area_cd;
        this.phone_num = phone_num;
        this.race_cd = race_cd;
        this.race_desc = race_desc;
        this.ethnic_cd = ethnic_cd;
        this.ethnic_desc = ethnic_desc;
        this.party_cd = party_cd;
        this.party_desc = party_desc;
        this.sex_code = sex_code;
        this.sex = sex;
        this.age = age;
        this.age_group = age_group;
        this.name_prefix = name_prefix;
        this.name_suffix = name_suffix;
        this.half_code = half_code;
        this.street_type = street_type;
        this.street_suffix = street_suffix;
        this.unit_designator = unit_designator;
        this.unit_num = unit_num;
        this.mail_addr1 = mail_addr1;
        this.mail_addr2 = mail_addr2;
        this.mail_addr3 = mail_addr3;
        this.mail_addr4 = mail_addr4;
        this.mail_city = mail_city;
        this.mail_state = mail_state;
        this.mail_zipcode = mail_zipcode;
    }

    @Override
    public String toString() {
        return "ReplacementEntry{" +
                "id=" + id +
                ", nc_id='" + nc_id + '\'' +
                ", timestamp=" + timestamp +
                ", county_id='" + county_id + '\'' +
                ", county_desc='" + county_desc + '\'' +
                ", last_name='" + last_name + '\'' +
                ", first_name='" + first_name + '\'' +
                ", midl_name='" + midl_name + '\'' +
                ", house_num='" + house_num + '\'' +
                ", street_dir='" + street_dir + '\'' +
                ", street_name='" + street_name + '\'' +
                ", res_city_desc='" + res_city_desc + '\'' +
                ", state_cd='" + state_cd + '\'' +
                ", zip_code='" + zip_code + '\'' +
                ", area_cd='" + area_cd + '\'' +
                ", phone_num='" + phone_num + '\'' +
                ", race_cd='" + race_cd + '\'' +
                ", race_desc='" + race_desc + '\'' +
                ", ethnic_cd='" + ethnic_cd + '\'' +
                ", ethnic_desc='" + ethnic_desc + '\'' +
                ", party_cd='" + party_cd + '\'' +
                ", party_desc='" + party_desc + '\'' +
                ", sex_code='" + sex_code + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                ", age_group='" + age_group + '\'' +
                ", name_prefix='" + name_prefix + '\'' +
                ", name_suffix='" + name_suffix + '\'' +
                ", half_code='" + half_code + '\'' +
                ", street_type='" + street_type + '\'' +
                ", street_suffix='" + street_suffix + '\'' +
                ", unit_designator='" + unit_designator + '\'' +
                ", unit_uum='" + unit_num + '\'' +
                ", mail_addr1='" + mail_addr1 + '\'' +
                ", mail_addr2='" + mail_addr2 + '\'' +
                ", mail_addr3='" + mail_addr3 + '\'' +
                ", mail_addr4='" + mail_addr4 + '\'' +
                ", mail_city='" + mail_city + '\'' +
                ", mail_state='" + mail_state + '\'' +
                ", mail_zipcode='" + mail_zipcode + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNc_id() {
        return nc_id;
    }

    public void setNc_id(String nc_id) {
        this.nc_id = nc_id;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getCounty_id() {
        return county_id;
    }

    public void setCounty_id(String county_id) {
        this.county_id = county_id;
    }

    public String getCounty_desc() {
        return county_desc;
    }

    public void setCounty_desc(String county_desc) {
        this.county_desc = county_desc;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getMidl_name() {
        return midl_name;
    }

    public void setMidl_name(String midl_name) {
        this.midl_name = midl_name;
    }

    public String getHouse_num() {
        return house_num;
    }

    public void setHouse_num(String house_num) {
        this.house_num = house_num;
    }

    public String getStreet_dir() {
        return street_dir;
    }

    public void setStreet_dir(String street_dir) {
        this.street_dir = street_dir;
    }

    public String getStreet_name() {
        return street_name;
    }

    public void setStreet_name(String street_name) {
        this.street_name = street_name;
    }

    public String getRes_city_desc() {
        return res_city_desc;
    }

    public void setRes_city_desc(String res_city_desc) {
        this.res_city_desc = res_city_desc;
    }

    public String getState_cd() {
        return state_cd;
    }

    public void setState_cd(String state_cd) {
        this.state_cd = state_cd;
    }

    public String getZip_code() {
        return zip_code;
    }

    public void setZip_code(String zip_code) {
        this.zip_code = zip_code;
    }

    public String getArea_cd() {
        return area_cd;
    }

    public void setArea_cd(String area_cd) {
        this.area_cd = area_cd;
    }

    public String getPhone_num() {
        return phone_num;
    }

    public void setPhone_num(String phone_num) {
        this.phone_num = phone_num;
    }

    public String getRace_cd() {
        return race_cd;
    }

    public void setRace_cd(String race_cd) {
        this.race_cd = race_cd;
    }

    public String getRace_desc() {
        return race_desc;
    }

    public void setRace_desc(String race_desc) {
        this.race_desc = race_desc;
    }

    public String getEthnic_cd() {
        return ethnic_cd;
    }

    public void setEthnic_cd(String ethnic_cd) {
        this.ethnic_cd = ethnic_cd;
    }

    public String getEthnic_desc() {
        return ethnic_desc;
    }

    public void setEthnic_desc(String ethnic_desc) {
        this.ethnic_desc = ethnic_desc;
    }

    public String getParty_cd() {
        return party_cd;
    }

    public void setParty_cd(String party_cd) {
        this.party_cd = party_cd;
    }

    public String getParty_desc() {
        return party_desc;
    }

    public void setParty_desc(String party_desc) {
        this.party_desc = party_desc;
    }

    public String getSex_code() {
        return sex_code;
    }

    public void setSex_code(String sex_code) {
        this.sex_code = sex_code;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getAge_group() {
        return age_group;
    }

    public void setAge_group(String age_group) {
        this.age_group = age_group;
    }

    public String getName_prefix() {
        return name_prefix;
    }

    public void setName_prefix(String name_prefix) {
        this.name_prefix = name_prefix;
    }

    public String getName_suffix() {
        return name_suffix;
    }

    public void setName_suffix(String name_suffix) {
        this.name_suffix = name_suffix;
    }

    public String getHalf_code() {
        return half_code;
    }

    public void setHalf_code(String half_code) {
        this.half_code = half_code;
    }

    public String getStreet_type() {
        return street_type;
    }

    public void setStreet_type(String street_type) {
        this.street_type = street_type;
    }

    public String getStreet_suffix() {
        return street_suffix;
    }

    public void setStreet_suffix(String street_suffix) {
        this.street_suffix = street_suffix;
    }

    public String getUnit_designator() {
        return unit_designator;
    }

    public void setUnit_designator(String unit_designator) {
        this.unit_designator = unit_designator;
    }

    public String getUnit_num() {
        return unit_num;
    }

    public void setUnit_num(String unit_num) {
        this.unit_num = unit_num;
    }

    public String getMail_addr1() {
        return mail_addr1;
    }

    public void setMail_addr1(String mail_addr1) {
        this.mail_addr1 = mail_addr1;
    }

    public String getMail_addr2() {
        return mail_addr2;
    }

    public void setMail_addr2(String mail_addr2) {
        this.mail_addr2 = mail_addr2;
    }

    public String getMail_addr3() {
        return mail_addr3;
    }

    public void setMail_addr3(String mail_addr3) {
        this.mail_addr3 = mail_addr3;
    }

    public String getMail_addr4() {
        return mail_addr4;
    }

    public void setMail_addr4(String mail_addr4) {
        this.mail_addr4 = mail_addr4;
    }

    public String getMail_city() {
        return mail_city;
    }

    public void setMail_city(String mail_city) {
        this.mail_city = mail_city;
    }

    public String getMail_state() {
        return mail_state;
    }

    public void setMail_state(String mail_state) {
        this.mail_state = mail_state;
    }

    public String getMail_zipcode() {
        return mail_zipcode;
    }

    public void setMail_zipcode(String mail_zipcode) {
        this.mail_zipcode = mail_zipcode;
    }
}

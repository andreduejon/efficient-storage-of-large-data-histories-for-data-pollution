package de.flinkmath;

import java.time.LocalDateTime;

public class Checkpoint {
    private String ncid;
    private int checkpointId;
    private int lastUpdate;
    private LocalDateTime timestamp;
    private String countyId;
    private String countyDesc;
    private String lastName;
    private String firstName;
    private String midlName;
    private String HouseNum;
    private String streetDir;
    private String streetName;
    private String resCityDesc;
    private String stateCd;
    private String zipCode;
    private String areaCd;
    private String phoneNum;
    private String raceCd;
    private String raceDesc;
    private String ethnicCd;
    private String ethnicDesc;
    private String partyCd;
    private String partyDesc;
    private String sexCode;
    private String sex;
    private String age;
    private String ageGroup;
    private String namePrefix;
    private String nameSuffix;
    private String halfCode;
    private String streetType;
    private String streetSuffix;
    private String unitDesignator;
    private String unitNum;
    private String mailAddr1;
    private String mailAddr2;
    private String mailAddr3;
    private String mailAddr4;
    private String mailCity;
    private String mailState;
    private String mailZipCode;

    public Checkpoint(String ncid, int checkpointId, int lastUpdate, LocalDateTime timestamp, String countyId,
                      String countyDesc, String lastName, String firstName, String midlName, String houseNum,
                      String streetDir, String streetName, String resCityDesc, String stateCd, String zipCode,
                      String areaCd, String phoneNum, String raceCd, String raceDesc, String ethnicCd,
                      String ethnicDesc, String partyCd, String partyDesc, String sexCode, String sex, String age,
                      String ageGroup, String namePrefix, String nameSuffix, String halfCode, String streetType,
                      String streetSuffix, String unitDesignator, String unitNum, String mailAddr1, String mailAddr2,
                      String mailAddr3, String mailAddr4, String mailCity, String mailState, String mailZipCode) {
        this.ncid = ncid;
        this.checkpointId = checkpointId;
        this.lastUpdate = lastUpdate;
        this.timestamp = timestamp;
        this.countyId = countyId;
        this.countyDesc = countyDesc;
        this.lastName = lastName;
        this.firstName = firstName;
        this.midlName = midlName;
        HouseNum = houseNum;
        this.streetDir = streetDir;
        this.streetName = streetName;
        this.resCityDesc = resCityDesc;
        this.stateCd = stateCd;
        this.zipCode = zipCode;
        this.areaCd = areaCd;
        this.phoneNum = phoneNum;
        this.raceCd = raceCd;
        this.raceDesc = raceDesc;
        this.ethnicCd = ethnicCd;
        this.ethnicDesc = ethnicDesc;
        this.partyCd = partyCd;
        this.partyDesc = partyDesc;
        this.sexCode = sexCode;
        this.sex = sex;
        this.age = age;
        this.ageGroup = ageGroup;
        this.namePrefix = namePrefix;
        this.nameSuffix = nameSuffix;
        this.halfCode = halfCode;
        this.streetType = streetType;
        this.streetSuffix = streetSuffix;
        this.unitDesignator = unitDesignator;
        this.unitNum = unitNum;
        this.mailAddr1 = mailAddr1;
        this.mailAddr2 = mailAddr2;
        this.mailAddr3 = mailAddr3;
        this.mailAddr4 = mailAddr4;
        this.mailCity = mailCity;
        this.mailState = mailState;
        this.mailZipCode = mailZipCode;
    }

    public String getNcid() {
        return ncid;
    }

    public void setNcid(String ncid) {
        this.ncid = ncid;
    }

    public int getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(int checkpointId) {
        this.checkpointId = checkpointId;
    }

    public int getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(int lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getCountyId() {
        return countyId;
    }

    public void setCountyId(String countyId) {
        this.countyId = countyId;
    }

    public String getCountyDesc() {
        return countyDesc;
    }

    public void setCountyDesc(String countyDesc) {
        this.countyDesc = countyDesc;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getMidlName() {
        return midlName;
    }

    public void setMidlName(String midlName) {
        this.midlName = midlName;
    }

    public String getHouseNum() {
        return HouseNum;
    }

    public void setHouseNum(String houseNum) {
        HouseNum = houseNum;
    }

    public String getStreetDir() {
        return streetDir;
    }

    public void setStreetDir(String streetDir) {
        this.streetDir = streetDir;
    }

    public String getStreetName() {
        return streetName;
    }

    public void setStreetName(String streetName) {
        this.streetName = streetName;
    }

    public String getResCityDesc() {
        return resCityDesc;
    }

    public void setResCityDesc(String resCityDesc) {
        this.resCityDesc = resCityDesc;
    }

    public String getStateCd() {
        return stateCd;
    }

    public void setStateCd(String stateCd) {
        this.stateCd = stateCd;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getAreaCd() {
        return areaCd;
    }

    public void setAreaCd(String areaCd) {
        this.areaCd = areaCd;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public String getRaceCd() {
        return raceCd;
    }

    public void setRaceCd(String raceCd) {
        this.raceCd = raceCd;
    }

    public String getRaceDesc() {
        return raceDesc;
    }

    public void setRaceDesc(String raceDesc) {
        this.raceDesc = raceDesc;
    }

    public String getEthnicCd() {
        return ethnicCd;
    }

    public void setEthnicCd(String ethnicCd) {
        this.ethnicCd = ethnicCd;
    }

    public String getEthnicDesc() {
        return ethnicDesc;
    }

    public void setEthnicDesc(String ethnicDesc) {
        this.ethnicDesc = ethnicDesc;
    }

    public String getPartyCd() {
        return partyCd;
    }

    public void setPartyCd(String partyCd) {
        this.partyCd = partyCd;
    }

    public String getPartyDesc() {
        return partyDesc;
    }

    public void setPartyDesc(String partyDesc) {
        this.partyDesc = partyDesc;
    }

    public String getSexCode() {
        return sexCode;
    }

    public void setSexCode(String sexCode) {
        this.sexCode = sexCode;
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

    public String getAgeGroup() {
        return ageGroup;
    }

    public void setAgeGroup(String ageGroup) {
        this.ageGroup = ageGroup;
    }

    public String getNamePrefix() {
        return namePrefix;
    }

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public String getNameSuffix() {
        return nameSuffix;
    }

    public void setNameSuffix(String nameSuffix) {
        this.nameSuffix = nameSuffix;
    }

    public String getHalfCode() {
        return halfCode;
    }

    public void setHalfCode(String halfCode) {
        this.halfCode = halfCode;
    }

    public String getStreetType() {
        return streetType;
    }

    public void setStreetType(String streetType) {
        this.streetType = streetType;
    }

    public String getStreetSuffix() {
        return streetSuffix;
    }

    public void setStreetSuffix(String streetSuffix) {
        this.streetSuffix = streetSuffix;
    }

    public String getUnitDesignator() {
        return unitDesignator;
    }

    public void setUnitDesignator(String unitDesignator) {
        this.unitDesignator = unitDesignator;
    }

    public String getUnitNum() {
        return unitNum;
    }

    public void setUnitNum(String unitNum) {
        this.unitNum = unitNum;
    }

    public String getMailAddr1() {
        return mailAddr1;
    }

    public void setMailAddr1(String mailAddr1) {
        this.mailAddr1 = mailAddr1;
    }

    public String getMailAddr2() {
        return mailAddr2;
    }

    public void setMailAddr2(String mailAddr2) {
        this.mailAddr2 = mailAddr2;
    }

    public String getMailAddr3() {
        return mailAddr3;
    }

    public void setMailAddr3(String mailAddr3) {
        this.mailAddr3 = mailAddr3;
    }

    public String getMailAddr4() {
        return mailAddr4;
    }

    public void setMailAddr4(String mailAddr4) {
        this.mailAddr4 = mailAddr4;
    }

    public String getMailCity() {
        return mailCity;
    }

    public void setMailCity(String mailCity) {
        this.mailCity = mailCity;
    }

    public String getMailState() {
        return mailState;
    }

    public void setMailState(String mailState) {
        this.mailState = mailState;
    }

    public String getMailZipCode() {
        return mailZipCode;
    }

    public void setMailZipCode(String mailZipCode) {
        this.mailZipCode = mailZipCode;
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
                "ncid='" + ncid + '\'' +
                ", checkpointId=" + checkpointId +
                ", lastUpdate=" + lastUpdate +
                ", timestamp=" + timestamp +
                ", countyId='" + countyId + '\'' +
                ", countyDesc='" + countyDesc + '\'' +
                ", lastName='" + lastName + '\'' +
                ", firstName='" + firstName + '\'' +
                ", midlName='" + midlName + '\'' +
                ", HouseNum='" + HouseNum + '\'' +
                ", streetDir='" + streetDir + '\'' +
                ", streetName='" + streetName + '\'' +
                ", resCityDesc='" + resCityDesc + '\'' +
                ", stateCd='" + stateCd + '\'' +
                ", zipCode='" + zipCode + '\'' +
                ", areaCd='" + areaCd + '\'' +
                ", phoneNum='" + phoneNum + '\'' +
                ", raceCd='" + raceCd + '\'' +
                ", raceDesc='" + raceDesc + '\'' +
                ", ethnicCd='" + ethnicCd + '\'' +
                ", ethnicDesc='" + ethnicDesc + '\'' +
                ", partyCd='" + partyCd + '\'' +
                ", partyDesc='" + partyDesc + '\'' +
                ", sexCode='" + sexCode + '\'' +
                ", sex='" + sex + '\'' +
                ", age='" + age + '\'' +
                ", ageGroup='" + ageGroup + '\'' +
                ", namePrefix='" + namePrefix + '\'' +
                ", nameSuffix='" + nameSuffix + '\'' +
                ", halfCode='" + halfCode + '\'' +
                ", streetType='" + streetType + '\'' +
                ", streetSuffix='" + streetSuffix + '\'' +
                ", unitDesignator='" + unitDesignator + '\'' +
                ", unitNum='" + unitNum + '\'' +
                ", mailAddr1='" + mailAddr1 + '\'' +
                ", mailAddr2='" + mailAddr2 + '\'' +
                ", mailAddr3='" + mailAddr3 + '\'' +
                ", mailAddr4='" + mailAddr4 + '\'' +
                ", mailCity='" + mailCity + '\'' +
                ", mailState='" + mailState + '\'' +
                ", mailZipCode='" + mailZipCode + '\'' +
                '}';
    }
}

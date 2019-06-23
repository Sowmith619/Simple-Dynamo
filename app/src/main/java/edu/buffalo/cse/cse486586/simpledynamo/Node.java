package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.*;

public class Node implements java.io.Serializable {
    private String currentPort;

    private String operationType;
    private String message;
    private String value;
    private String nextPort;
    private String nextNextPort;
    private String pp;
    private String ppp;
    private String type1;
    private String prevPort;
    private String allrep;
    private String version;
    private String prevPrev;

    public String getPrevPrev() {
        return prevPrev;
    }

    public void setPrevPrev(String prevPrev) {
        this.prevPrev = prevPrev;
    }



    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    private HashMap<String,String> forAll;

    public String getType1() {
        return type1;
    }

    public void setType1(String type) {
        this.type1 = type;
    }


    public String getPrevPort() {
        return prevPort;
    }

    public void setPrevPort(String prevPort) {
        this.prevPort = prevPort;
    }



    public String getAllrep() {
        return allrep;
    }

    public void setAllrep(String allrep) {
        this.allrep = allrep;
    }



    public String getPp() {
        return pp;
    }

    public void setPp(String pp) {
        this.pp = pp;
    }

    public String getPpp() {
        return ppp;
    }

    public void setPpp(String ppp) {
        this.ppp = ppp;
    }



    public HashMap<String, String> getForAll() {
        return forAll;
    }

    public void setForAll(HashMap<String, String> forAll) {
        this.forAll = forAll;
    }



    public String getNextPort() {
        return nextPort;
    }

    public void setNextPort(String nextPort) {
        this.nextPort = nextPort;
    }

    public String getNextNextPort() {
        return nextNextPort;
    }

    public void setNextNextPort(String nextNextPort) {
        this.nextNextPort = nextNextPort;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }




    public String getCurrentPort() {
        return currentPort;
    }

    public void setCurrentPort(String currentPort) {
        this.currentPort = currentPort;
    }



    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }
}


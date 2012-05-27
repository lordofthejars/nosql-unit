package com.lordofthejars.nosqlunit.core;

import static com.lordofthejars.nosqlunit.core.OperatingSystemFamily.*;

public enum OperatingSystem {
    LINUX_OS ("Linux",LINUX),
    MAC_OSX ("Mac OS X",MAC),
    MAC_OS ("Mac OS",MAC),
    WINDOWS_95 ("Windows 95",WINDOWS),
    WINDOWS_98 ("Windows 98",WINDOWS),
    WINDOWS_ME ("Windows Me",WINDOWS),
    WINDOWS_NT ("Windows NT",WINDOWS),
    WINDOWS_2000 ("Windows 2000",WINDOWS),
    WINDOWS_XP ("Windows XP",WINDOWS),
    WINDOWS_7 ("Windows 7",WINDOWS),
    WINDOWS_2003 ("Windows 2003",WINDOWS),
    WINDOWS_2008 ("Windows 2008",WINDOWS),
    SUN_OS ("Sun OS ",UNIX),
    MPE_IX ("MPE/iX",UNIX),
    HP_UX ("HP-UX",UNIX),
    AIX ("AIX",UNIX),
    OS_390 ("OS/390",UNIX),
    FREEBSD ("FreeBSD",UNIX),
    IRIX ("Irix",UNIX),
    DIGITAL_UNIX ("Digital Unix",UNIX),
    NETWARE_4_11 ("NetWare 4.11",UNIX),
    OSF1 ("OSF1",UNIX),
    OPENVMS ("OpenVMS",DEC_OS),
    UNKNOWN_OS("Unknown", UNKNOWN);
    
    
    final private String  label;
    final private OperatingSystemFamily family;
    
    
    private OperatingSystem(String label, OperatingSystemFamily family) {
        this.label = label;
        this.family = family;
    }
    public String getLabel() {
        return label;
    }
    public OperatingSystemFamily getFamily() {
        return family;
    }
    static public OperatingSystem resolve(String osName){
        for (OperatingSystem os : OperatingSystem.values()) {
            if (os.label.equalsIgnoreCase(osName)) return os;
        }
        return OperatingSystem.UNKNOWN_OS;
    }
    
}
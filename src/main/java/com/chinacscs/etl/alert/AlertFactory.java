package com.chinacscs.etl.alert;

import com.chinacscs.utils.PropUtils;
import com.chinacscs.utils.StringUtils;

import java.lang.reflect.Constructor;

/**
 * @author wangjj
 * @date 2017/8/11 20:48
 * @description Alert对象工厂
 * @copyright(c) chinacscs all rights reserved
 */
public class AlertFactory {
    public static IAlert getAlert() throws Exception {
        String alertType = PropUtils.getString("alert.type");
        if(StringUtils.isEmpty(alertType)) {
            alertType = "AlertByMail";
        }
        String alertClassName = AlertFactory.class.getPackage().getName() + "." + alertType;
        Class clazz = Class.forName(alertClassName);
        Constructor constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        return (IAlert)constructor.newInstance();
    }
}

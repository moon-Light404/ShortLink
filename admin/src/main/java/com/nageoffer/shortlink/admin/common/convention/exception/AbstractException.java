package com.nageoffer.shortlink.admin.common.convention.exception;

import com.nageoffer.shortlink.admin.common.convention.errorcode.IErrorCode;
import lombok.Getter;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * 抽象项目中三类异常体系，客户端异常、服务端异常以及远程服务调用异常
 *
 * @see ClientException
 * @see ServiceException
 * @see RemoteException
 */

@Getter
public abstract class AbstractException extends RuntimeException{

    public final String errorCode;

    public final String errorMessage;

    public AbstractException(String message, Throwable throwable, IErrorCode errorCode){
        super(message, throwable);
        // 记录errorCode
        this.errorCode = errorCode.code();
        // 优先使用message，message如果为空则使用errorCode
        this.errorMessage = Optional.ofNullable(StringUtils.hasLength(message) ? message : null).orElse(errorCode.message());
    }
}

package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.rds.model.DBProxy;
import com.amazonaws.services.rds.model.UserAuthConfig;
import com.amazonaws.services.rds.model.UserAuthConfigInfo;

public class Utility {
    static <A, B> List<B> map(Collection<A> xs, Function<A, B> f) {
        return Optional.ofNullable(xs).orElse(Collections.emptyList()).stream().map(f)
                       .collect(Collectors.toList());
    }

    public static ResourceModel resultToModel(DBProxy proxy){
        List<AuthFormat> authModels = proxy.getAuth().stream().map(a -> resultToModel(a)).collect(Collectors.toList());
        return ResourceModel
                       .builder()
                       .auth(authModels)
                       .dBProxyArn(proxy.getDBProxyArn())
                       .dBProxyName(proxy.getDBProxyName())
                       .debugLogging(proxy.getDebugLogging())
                       .endpoint(proxy.getEndpoint())
                       .engineFamily(proxy.getEngineFamily())
                       .idleClientTimeout(proxy.getIdleClientTimeout())
                       .requireTLS(proxy.getRequireTLS())
                       .roleArn(proxy.getRoleArn())
                       .vpcSecurityGroupIds(proxy.getVpcSecurityGroupIds())
                       .vpcSubnetIds(proxy.getVpcSecurityGroupIds())
                       .build();
    }

    public static AuthFormat resultToModel(UserAuthConfigInfo auth) {
        return AuthFormat.builder()
                   .authScheme(auth.getAuthScheme())
                   .description(auth.getDescription())
                   .iAMAuth(auth.getIAMAuth())
                   .secretArn(auth.getSecretArn())
                   .userName(auth.getUserName())
                   .build();
    }

    public static List<UserAuthConfig> getUserAuthConfigs(ResourceModel model) {
        List<UserAuthConfig> userAuthConfigList = new ArrayList<>();

        if (model.getAuth() == null) {
            return userAuthConfigList;
        }

        for(AuthFormat auth : model.getAuth()){
            UserAuthConfig uac = new UserAuthConfig()
                                         .withAuthScheme(auth.getAuthScheme())
                                         .withDescription(auth.getDescription())
                                         .withIAMAuth(auth.getIAMAuth())
                                         .withSecretArn(auth.getSecretArn())
                                         .withUserName(auth.getUserName());
            userAuthConfigList.add(uac);
        }
        return userAuthConfigList;
    }
}

package software.amazon.rds.dbproxy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.UserAuthConfig;
import software.amazon.awssdk.services.rds.model.UserAuthConfigInfo;


public class Utility {
    static <A, B> List<B> map(Collection<A> xs, Function<A, B> f) {
        return Optional.ofNullable(xs).orElse(Collections.emptyList()).stream().map(f)
                .collect(Collectors.toList());
    }

    public static ResourceModel resultToModel(DBProxy proxy) {
        List<AuthFormat> authModels = proxy.auth().stream().map(a -> resultToModel(a)).collect(Collectors.toList());
        return ResourceModel
                .builder()
                .auth(authModels)
                .dBProxyArn(proxy.dbProxyArn())
                .dBProxyName(proxy.dbProxyName())
                .debugLogging(proxy.debugLogging())
                .endpoint(proxy.endpoint())
                .engineFamily(proxy.engineFamily())
                .idleClientTimeout(proxy.idleClientTimeout())
                .requireTLS(proxy.requireTLS())
                .roleArn(proxy.roleArn())
                .vpcId(proxy.vpcId())
                .vpcSecurityGroupIds(proxy.vpcSecurityGroupIds())
                .vpcSubnetIds(proxy.vpcSubnetIds())
                .build();
    }

    public static AuthFormat resultToModel(UserAuthConfigInfo auth) {
        return AuthFormat.builder()
                .authScheme(auth.authScheme() != null ? auth.authScheme().toString() : null)
                .description(auth.description())
                .iAMAuth(auth.iamAuth() != null ? auth.iamAuth().toString() : null)
                .secretArn(auth.secretArn())
                .clientPasswordAuthType(auth.clientPasswordAuthType() != null ? auth.clientPasswordAuthType().toString() : null)
                .build();
    }

    public static List<UserAuthConfig> getUserAuthConfigs(ResourceModel model) {
        List<UserAuthConfig> userAuthConfigList = new ArrayList<>();

        if (model.getAuth() == null) {
            return userAuthConfigList;
        }

        for (AuthFormat auth : model.getAuth()) {
            UserAuthConfig uac = UserAuthConfig.builder()
                    .authScheme(auth.getAuthScheme())
                    .description(auth.getDescription())
                    .iamAuth(auth.getIAMAuth())
                    .secretArn(auth.getSecretArn())
                    .clientPasswordAuthType(auth.getClientPasswordAuthType())
                    .build();
            userAuthConfigList.add(uac);
        }
        return userAuthConfigList;
    }
}

package software.amazon.rds.dbproxyendpoint;

import java.util.HashSet;
import java.util.List;

import software.amazon.awssdk.services.rds.model.DBProxyEndpoint;


public class Utility {

    public static ResourceModel resultToModel(DBProxyEndpoint proxyEndpoint){
        return ResourceModel
                .builder()
                .dBProxyEndpointArn(proxyEndpoint.dbProxyEndpointArn())
                .dBProxyEndpointName(proxyEndpoint.dbProxyEndpointName())
                .dBProxyName(proxyEndpoint.dbProxyName())
                .vpcId(proxyEndpoint.vpcId())
                .vpcSecurityGroupIds(proxyEndpoint.vpcSecurityGroupIds())
                .vpcSubnetIds(proxyEndpoint.vpcSubnetIds())
                .endpoint(proxyEndpoint.endpoint())
                .targetRole(proxyEndpoint.targetRole() != null ? proxyEndpoint.targetRole().toString() : null)
                .isDefault(proxyEndpoint.isDefault())
                .build();
    }

    public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
        return new HashSet<>(list1).equals(new HashSet<>(list2));
    }
}

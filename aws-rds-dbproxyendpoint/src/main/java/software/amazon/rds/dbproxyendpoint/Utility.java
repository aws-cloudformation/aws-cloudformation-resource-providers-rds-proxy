package software.amazon.rds.dbproxyendpoint;

import java.util.HashSet;
import java.util.List;

import com.amazonaws.services.rds.model.DBProxyEndpoint;

public class Utility {

    public static ResourceModel resultToModel(DBProxyEndpoint proxyEndpoint){
        return ResourceModel
                .builder()
                .dBProxyEndpointArn(proxyEndpoint.getDBProxyEndpointArn())
                .dBProxyEndpointName(proxyEndpoint.getDBProxyEndpointName())
                .dBProxyName(proxyEndpoint.getDBProxyName())
                .vpcId(proxyEndpoint.getVpcId())
                .vpcSecurityGroupIds(proxyEndpoint.getVpcSecurityGroupIds())
                .vpcSubnetIds(proxyEndpoint.getVpcSubnetIds())
                .endpoint(proxyEndpoint.getEndpoint())
                .targetRole(proxyEndpoint.getTargetRole())
                .isDefault(proxyEndpoint.getIsDefault())
                .build();
    }

    public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
        return new HashSet<>(list1).equals(new HashSet<>(list2));
    }
}

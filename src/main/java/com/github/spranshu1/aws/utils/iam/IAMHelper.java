/*
 * Created By: Pranshu Shrivastava

 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.spranshu1.aws.utils.iam;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.*;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The Identity Management Helper
 */
public class IAMHelper {

    private final AmazonIdentityManagement amazonIdentityManagement;

    /**
     * Instantiates a new Iam discovery.
     *
     * @param amazonIdentityManagement the amazon identity management
     */
    public IAMHelper(AmazonIdentityManagement amazonIdentityManagement) {
        this.amazonIdentityManagement = amazonIdentityManagement;
    }

    /**
     * List account aliases list.
     *
     * @return the list
     */
    public List<String> listAccountAliases() {
        return amazonIdentityManagement.listAccountAliases().getAccountAliases();
    }

    /**
     * Check if role is present.
     *
     * @param roleName the role name
     * @return the boolean
     */
    public boolean hasRole(final String roleName) {
        return getRoleArn(roleName).isPresent();
    }

    /**
     * Gets role arn.
     *
     * @param roleName the role name
     * @return the role arn
     */
    public Optional<String> getRoleArn(final String roleName) {
        try {
            GetRoleRequest request = new GetRoleRequest()
                    .withRoleName(roleName);
            GetRoleResult result = amazonIdentityManagement.getRole(request);
            return Optional.of(result.getRole().getArn());
        } catch (AmazonIdentityManagementException e) {
            return Optional.empty();
        }
    }

    /**
     * Has instance profile.
     *
     * @param instanceProfileName the instance profile name
     * @return the boolean
     */
    public boolean hasInstanceProfile(final String instanceProfileName) {
        return getInstanceProfileArn(instanceProfileName).isPresent();
    }

    /**
     * Gets instance profile arn.
     *
     * @param instanceProfileName the instance profile name
     * @return the instance profile arn
     */
    public Optional<String> getInstanceProfileArn(final String instanceProfileName) {
        try {
            final GetInstanceProfileRequest request = new GetInstanceProfileRequest()
                    .withInstanceProfileName(instanceProfileName);
            final GetInstanceProfileResult result = amazonIdentityManagement.getInstanceProfile(request);
            return Optional.of(result.getInstanceProfile().getArn());
        } catch (AmazonIdentityManagementException e) {
            return Optional.empty();
        }
    }

    /**
     * Has policy.
     *
     * @param name the name
     * @return the boolean
     */
    public boolean hasPolicy(final String name) {
        return getPolicyArn(name).isPresent();
    }

    /**
     * Gets policy arn.
     *
     * @param policyName the policy name
     * @return the policy arn
     */
    public Optional<String> getPolicyArn(final String policyName) {
        String marker = null;
        Optional<String> policy;
        ListPoliciesResult result;
        do {
            result = amazonIdentityManagement.listPolicies(new ListPoliciesRequest()
                    .withMarker(marker));
            policy = result.getPolicies().stream()
                    .filter(p -> policyName.equals(p.getPolicyName()))
                    .findFirst()
                    .map(Policy::getArn);
            marker = result.getMarker();
        } while (!policy.isPresent() && result.isTruncated());

        return policy;
    }

    /**
     * Gets attached policies arns for role.
     *
     * @param roleName the role name
     * @return the attached policies arns for role
     */
    public List<String> getAttachedPoliciesArnsForRole(final String roleName) {
        final ListAttachedRolePoliciesRequest request = new ListAttachedRolePoliciesRequest().withRoleName(roleName);
        try {
            ListAttachedRolePoliciesResult result = amazonIdentityManagement.listAttachedRolePolicies(request);
            return result.getAttachedPolicies()
                    .stream()
                    .map(AttachedPolicy::getPolicyArn)
                    .collect(Collectors.toList());
        } catch (AmazonIdentityManagementException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Gets instance profile roles.
     *
     * @param profileName the profile name
     * @return the instance profile roles
     */
    public List<String> getInstanceProfileRoles(final String profileName) {
        GetInstanceProfileRequest request = new GetInstanceProfileRequest().withInstanceProfileName(profileName);
        try {
            GetInstanceProfileResult result = amazonIdentityManagement.getInstanceProfile(request);
            return result.getInstanceProfile()
                    .getRoles()
                    .stream()
                    .map(Role::getRoleName)
                    .collect(Collectors.toList());
        } catch (AmazonIdentityManagementException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Gets server certificate arn.
     *
     * @param certificateName the certificate name
     * @return the server certificate arn
     */
    public Optional<String> getServerCertificateArn(final String certificateName) {
        final ListServerCertificatesResult result = amazonIdentityManagement.listServerCertificates();
        return result.getServerCertificateMetadataList()
                .stream()
                .filter(serviceCertificate ->
                        Objects.equals(certificateName, serviceCertificate.getServerCertificateName()))
                .map(ServerCertificateMetadata::getArn)
                .findFirst();
    }
}
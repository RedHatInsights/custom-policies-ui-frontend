import parseJSON from 'date-fns/parseJSON';

import {
    PagedServerPolicyResponse,
    Policy,
    NewPolicy,
    ServerPolicyRequest,
    ServerPolicyResponse
} from '../types/Policy/Policy';
import { ActionType } from '../types/Policy/Actions';
import { assertNever } from './Assert';
import { DeepPartial } from 'ts-essentials';

export const toServerPolicy = (policy: DeepPartial<Policy>): ServerPolicyRequest => {

    const { isEnabled, ...restPolicy } = policy;

    return {
        ...restPolicy,
        is_enabled: isEnabled,
        actions: policy.actions?.map((action): string => {
            if (!action || !action.type) {
                return '';
            }

            let encodedAction = `${action.type}`;

            switch (action.type) {
                case ActionType.WEBHOOK:
                    encodedAction += ' ' + action.endpoint;
                    break;
                case ActionType.EMAIL:
                    break;
                default:
                    assertNever(action.type);
            }

            return encodedAction;
        }).join(';'),
        mtime: policy.mtime ? policy.mtime.toJSON() : undefined
    };
};

export const toPolicy = (serverPolicy: ServerPolicyResponse): Policy => {
    const { is_enabled: isEnabled, ...restServerPolicy } = serverPolicy;
    return {
        ...restServerPolicy,
        isEnabled,
        actions: [],
        mtime: parseJSON(serverPolicy.mtime)
    };
};

export const toPolicies = (serverPolicies: PagedServerPolicyResponse): Policy[] => {
    return serverPolicies.data.map(toPolicy);
};

export const makeCopyOfPolicy = (policy: Policy): NewPolicy => {
    return {
        ...policy,
        name: `Copy of ${policy.name}`,
        mtime: undefined,
        id: undefined,
        customerid: undefined
    };
};

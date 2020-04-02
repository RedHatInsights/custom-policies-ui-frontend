import Config from '../config/Config';
import { PagedServerPolicyResponse, Policy } from '../types/Policy/Policy';
import { Action, useMutation, useQuery } from 'react-fetching-library';
import { Fact } from '../types/Fact';
import { Page } from '../types/Page';
import { toPolicies, toServerPolicy } from '../utils/PolicyAdapter';
import { UsePaginatedQueryResponse, useTransformQueryResponse } from '../utils/ApiUtils';
import { DeepPartial } from 'ts-essentials';
import { useBulkMutation } from '../hooks';

const urls = Config.apis.urls;

type Method = 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'OPTIONS';

export const createAction = (method: Method, url: string, queryParams?: any, data?: any): Action => {
    const parsedURL = new URL(url, 'http://dummybase');
    const querySeparator = parsedURL.searchParams.toString() === '' ? '?' : '&';
    const queryString = queryParams ? new URLSearchParams(queryParams).toString() : '';

    const endpoint = url + (queryString === '' ? '' : querySeparator + queryString);

    return {
        method,
        endpoint,
        body: data
    };
};

export const useNewQuery = <T>(method: Method, url: string, initFetch?: boolean, queryParams?: any, data?: any) =>
    useQuery<T>(createAction(method, url, queryParams, data), initFetch);

const queryParamsPaginated = (queryParams?: any, page?: Page) => {
    if (!page) {
        page = Page.defaultPage();
    }

    if (!queryParams) {
        queryParams = { };
    }

    queryParams.offset = (page.index - 1) * page.size;
    queryParams.limit = page.size;

    if (page.filter) {
        for (const filterElement of page.filter.elements) {
            queryParams[`filter[${filterElement.column}]`] = filterElement.value;
            queryParams[`filter:op[${filterElement.column}]`] = filterElement.operator;
        }
    }

    if (page.sort) {
        queryParams.sortColumn = page.sort.column;
        queryParams.sortDirection = page.sort.direction;
    }

    return queryParams;
};

const useNewPaginatedQuery =
    <T>(method: Method, url: string, page?: Page, initFetch?: boolean, queryParams?: any, data?: any): UsePaginatedQueryResponse<T> => {
        const result = useNewQuery<T>(method, url, initFetch, queryParamsPaginated(queryParams, page), data);
        const itemCount = result.headers?.get('TotalCount');

        return { count: (itemCount ? +itemCount : itemCount) as number, ...result };
    };

export const useGetFactsQuery = (initFetch?: boolean) => useNewQuery<Fact[]>('GET', urls.facts, initFetch);

export const useGetPoliciesQuery = (page?: Page, initFetch?: boolean): UsePaginatedQueryResponse<Policy[]> => {
    return useTransformQueryResponse(
        useNewPaginatedQuery<PagedServerPolicyResponse>('GET', urls.policies, page, initFetch),
        toPolicies
    );
};

export const useVerifyPolicyMutation = () => {
    return useMutation((policy: DeepPartial<Policy>) => {
        return createAction('POST', urls.validateCondition, {}, toServerPolicy(policy));
    });
};

export const useBulkDeletePolicyMutation = () => {
    return useBulkMutation((policy: Policy) => {
        return createAction('DELETE', urls.policy(policy.id));
    });
};

export const useGetPolicyQuery = (policyId: string, initFetch?: boolean) =>
    useNewQuery<Policy>('GET', urls.policy(policyId), initFetch);

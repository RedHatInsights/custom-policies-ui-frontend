import * as React from 'react';
import { useContext } from 'react';
import { PageSection } from '@patternfly/react-core';
import { Main, PageHeader, PageHeaderTitle, Section } from '@redhat-cloud-services/frontend-components';

import { PolicyRow, PolicyTable } from '../../components/Policy/Table/PolicyTable';
import { useBulkSavePolicyMutation } from '../../services/useSavePolicy';
import { useGetPoliciesQuery } from '../../services/Api';
import { PolicyToolbar } from '../../components/Policy/TableToolbar/PolicyTableToolbar';
import { CreatePolicyWizard } from './CreatePolicyWizard';
import { AppContext } from '../../app/AppContext';
import { policyTableError } from './PolicyTableError';
import { ActionType, Policy } from '../../types/Policy';
import { DeletePolicy } from './DeletePolicy';
import { NewPolicy } from '../../types/Policy/Policy';
import { usePolicyFilter } from '../../hooks/usePolicyFilter';
import { usePolicyPage } from '../../hooks/usePolicyPage';
import { useSort } from '../../hooks/useSort';
import { usePolicyRows } from '../../hooks/usePolicyRows';
import { makeCopyOfPolicy } from '../../utils/PolicyAdapter';
import { PolicyFilterColumn } from '../../types/Policy/PolicyPaging';
import { EmailOptIn } from '../../components/EmailOptIn/EmailOptIn';
import { Messages } from '../../properties/Messages';

type ListPageProps = {};

type PolicyWizardStateBase = {
    template: NewPolicy | undefined;
    showCreateStep: boolean;
};

type PolicyWizardStateOpen = {
    isOpen: true;
} & PolicyWizardStateBase;

type PolicyWizardStateClosed = {
    isOpen: false;
} & Partial<PolicyWizardStateBase>;

type PolicyWizardState = PolicyWizardStateClosed | PolicyWizardStateOpen;

const ListPage: React.FunctionComponent<ListPageProps> = (_props) => {

    const [ policyWizardState, setPolicyWizardState ] = React.useState<PolicyWizardState>({
        isOpen: false
    });
    const [ policyToDelete, setPolicyToDelete ] = React.useState<Policy[]>();
    const bulkSavePolicyMutation = useBulkSavePolicyMutation();
    const policyFilters = usePolicyFilter();
    const sort = useSort();
    const policyPage = usePolicyPage(policyFilters.debouncedFilters, undefined, sort.sortBy);
    const getPoliciesQuery = useGetPoliciesQuery(policyPage.page, false);
    const appContext = useContext(AppContext);

    const isLoading = getPoliciesQuery.loading || bulkSavePolicyMutation.loading;

    const policyRows = usePolicyRows(getPoliciesQuery.payload, isLoading);

    const { canWriteAll, canReadAll } = appContext.rbac;

    const { query: getPoliciesQueryReload } = getPoliciesQuery;
    const { mutate: mutateSavePolicy, loading: loadingSavePolicy } = bulkSavePolicyMutation;

    const { changePage, currentPage } = policyPage;

    React.useEffect(() => {
        if (loadingSavePolicy === false) {
            getPoliciesQueryReload();
        }
    }, [ loadingSavePolicy, getPoliciesQueryReload ]);

    const onCloseDeletePolicy = React.useCallback((deleted: boolean) => {
        if (deleted) {
            getPoliciesQueryReload();
            if (policyToDelete?.length === getPoliciesQuery.payload?.length) {
                changePage(undefined, currentPage === 1 ? 1 : currentPage - 1);
            }
        }

        setPolicyToDelete(undefined);
    }, [ getPoliciesQueryReload, setPolicyToDelete, changePage, currentPage, policyToDelete, getPoliciesQuery.payload ]);

    const switchPolicyEnabled = (policy: Policy) => ({ ...policy, isEnabled: !policy.isEnabled });

    const tableActionsResolver = React.useCallback((policy: PolicyRow) => {
        if (!canWriteAll) {
            return [];
        }

        return [
            {
                title: `${policy.isEnabled ? 'Disable' : 'Enable'} policy`,
                onClick: () => {
                    mutateSavePolicy([ policy ].map(switchPolicyEnabled));
                }
            },
            {
                title: 'Edit',
                onClick: () => {
                    setPolicyWizardState({
                        isOpen: true,
                        template: policy,
                        showCreateStep: false
                    });
                }
            },
            {
                title: 'Duplicate',
                onClick: () => {
                    setPolicyWizardState({
                        isOpen: true,
                        template: makeCopyOfPolicy(policy),
                        showCreateStep: false
                    });
                }
            },
            {
                title: 'Delete',
                onClick: () => {
                    setPolicyToDelete([ policy ]);
                }
            }
        ];
    }, [ canWriteAll, setPolicyToDelete, mutateSavePolicy ]);

    React.useEffect(() => {
        if (canReadAll) {
            getPoliciesQueryReload();
        }
    }, [ canReadAll, getPoliciesQueryReload ]);

    const createCustomPolicy = React.useCallback(() => {
        setPolicyWizardState({
            isOpen: true,
            showCreateStep: true,
            template: undefined
        });
    }, [ setPolicyWizardState ]);

    const closeCustomPolicyWizard = React.useCallback((policyCreated: boolean) => {
        if (policyCreated) {
            getPoliciesQueryReload();
        }

        setPolicyWizardState({
            isOpen: false
        });
    }, [ setPolicyWizardState, getPoliciesQueryReload ]);

    const policyTableErrorValue = React.useMemo(
        () => {
            return policyTableError(
                canReadAll,
                {
                    clearAllFiltersAndTryAgain: () => {
                        policyFilters.setFilters[PolicyFilterColumn.NAME]('');
                        policyFilters.setFilters[PolicyFilterColumn.DESCRIPTION]('');
                        policyFilters.setFilters[PolicyFilterColumn.IS_ACTIVE]({
                            disabled: false,
                            enabled: false
                        });
                        changePage(undefined, 1);
                    },
                    refreshPage: () => {
                        window.location.reload();
                    },
                    tryAgain: () => {
                        getPoliciesQueryReload();
                    }
                },
                getPoliciesQuery.error,
                getPoliciesQuery.status
            );
        },
        [
            canReadAll,
            getPoliciesQuery.error,
            getPoliciesQuery.status,
            policyFilters.setFilters,
            changePage,
            getPoliciesQueryReload
        ]
    );

    const selectedPolicies = React.useCallback(() => policyRows.rows.filter(policy => policy.isSelected), [ policyRows ]);

    const onDeletePolicies = React.useCallback(
        () => setPolicyToDelete(selectedPolicies()),
        [ selectedPolicies, setPolicyToDelete ]
    );

    const onDisablePolicies = React.useCallback(
        () => mutateSavePolicy(selectedPolicies().map(p => ({ ...p, isEnabled: false }))),
        [ selectedPolicies, mutateSavePolicy ]
    );

    const onEnablePolicies = React.useCallback(
        () => mutateSavePolicy(selectedPolicies().map(p => ({ ...p, isEnabled: true }))),
        [ selectedPolicies, mutateSavePolicy ]
    );

    return (
        <>
            <PageHeader>
                <PageHeaderTitle title="Policies"/>
            </PageHeader>
            { appContext.userSettings &&
            !appContext.userSettings.dailyEmail &&
            getPoliciesQuery.payload &&
            getPoliciesQuery.payload.find(p => p.actions.find(a => a.type === ActionType.EMAIL)) && (
                <PageSection>
                    <EmailOptIn content={ Messages.pages.listPage.emailOptIn } />
                </PageSection>
            )}
            <Main>
                <Section>
                    <PolicyToolbar
                        onCreatePolicy={ canWriteAll ? createCustomPolicy : undefined }
                        onDeletePolicy={ canWriteAll ? onDeletePolicies : undefined }
                        onEnablePolicy={ canWriteAll ? onEnablePolicies : undefined }
                        onDisablePolicy={ canWriteAll ? onDisablePolicies : undefined }
                        onPaginationChanged={ policyPage.changePage }
                        onPaginationSizeChanged={ policyPage.changeItemsPerPage }
                        onSelectionChanged={ policyRows.onSelectionChanged }
                        selectedCount={ policyRows.selectionCount }
                        page={ policyPage.currentPage }
                        pageCount={ getPoliciesQuery.payload?.length }
                        perPage={ policyPage.itemsPerPage }
                        showPerPageOptions={ true }
                        filterElements={ policyFilters.filters }
                        setFilterElements = { policyFilters.setFilters }
                        clearFilters={ policyFilters.clearFilterHandler }
                        count={ getPoliciesQuery.count }
                    />
                    <PolicyTable
                        policies={ policyRows.rows }
                        onCollapse={ policyRows.onCollapse }
                        onSelect={ policyRows.onSelect }
                        actionResolver={ tableActionsResolver }
                        loading={ isLoading }
                        error={ policyTableErrorValue }
                        onSort={ sort.onSort }
                        sortBy={ sort.sortBy }
                    />
                </Section>
            </Main>
            { policyWizardState.isOpen && <CreatePolicyWizard
                isOpen={ policyWizardState.isOpen }
                close={ closeCustomPolicyWizard }
                initialValue={ policyWizardState.template }
                showCreateStep={ policyWizardState.showCreateStep }
            /> }
            <DeletePolicy onClose={ onCloseDeletePolicy } policies={ policyToDelete }/>
        </>
    );
};

export default ListPage;

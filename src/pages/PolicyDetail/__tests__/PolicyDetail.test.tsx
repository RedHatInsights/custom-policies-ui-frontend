import * as React from 'react';
import fetchMock  from 'fetch-mock';
import { render, screen, getByRole } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import inBrowserDownload from 'in-browser-download';
import { PolicyDetail } from '../PolicyDetail';
import { appWrapperCleanup, appWrapperSetup, getConfiguredAppWrapper } from '../../../../test/AppWrapper';
import { linkTo } from '../../../Routes';
import {
    actionDeletePoliciesIds,
    actionGetPoliciesById,
    actionGetPoliciesByIdHistoryTrigger, actionPostPolicies, actionPostPoliciesIdsEnabled, actionPostPoliciesValidate,
    actionPostPoliciesValidateName, actionPutPoliciesByPolicyId
} from '../../../generated/ActionCreators';
import { waitForAsyncEvents } from '../../../../test/TestUtils';
import { ServerPolicyRequest } from '../../../types/Policy/Policy';

jest.mock('../../../hooks/useFacts');
jest.mock('in-browser-download', () => jest.fn());
jest.mock('@redhat-cloud-services/frontend-components', () => {
    const MockedSkeletonTable = () => <div>Loading Triggers</div>;

    return {
        ...jest.requireActual('@redhat-cloud-services/frontend-components'),
        SkeletonTable: MockedSkeletonTable
    };
});

describe('src/Pages/PolicyDetail/PolicyDetail', () => {

    beforeEach(() => {
        appWrapperSetup();
    });

    afterEach(() => {
        appWrapperCleanup();
    });

    type FetchMockConfig = {
        policyStatus?: number;
        policyIsUndefined?: boolean;
        policyId?: string;
        policy?: any;
        triggers?: any;
        triggerLoading?: boolean;
        triggerOffset?: number;
        triggerLimit?: number;
        triggersCount?: number;
    };

    const mockPolicy = {
        actions: 'email',
        conditions: 'facts.arch = "x86_64"',
        ctime: '2020-06-02 16:11:09.622',
        description: 'Fail if we are running on a x86_64',
        id: 'foo',
        isEnabled: true,
        lastTriggered: 1591132435642,
        mtime: '2020-06-02 16:11:48.428',
        name: 'Not arch x86_64'
    };

    const mockTriggers = [
        {
            ctime: 1591132431400,
            hostName: 'foo-bar',
            id: 'my-stuff'
        },
        {
            ctime: 1591132404157,
            hostName: 'random host',
            id: 'foo-bar-id'
        },
        {
            ctime: 1591132300131,
            hostName: 'random host',
            id: 'foo-bar-id'
        },
        {
            ctime: 1591132299051,
            hostName: 'foo-bar',
            id: 'my-stuff'
        }
    ];

    const fetchMockSetup = (config?: FetchMockConfig) => {
        fetchMock.getOnce(actionGetPoliciesById({
            id: config?.policyId || 'foo'
        }).endpoint, {
            body: config?.policyIsUndefined === true ? undefined : (config?.policy || mockPolicy),
            status: config?.policyStatus || 200
        }, {
            overwriteRoutes: false
        });

        fetchMock.getOnce(actionGetPoliciesByIdHistoryTrigger({
            id: config?.policyId || 'foo',
            offset: config?.triggerOffset || 0,
            limit: config?.triggerLimit || 50
        }).endpoint, config?.triggerLoading === true ? new Promise(() => '') : {
            body: {
                data: (config?.triggers || mockTriggers),
                meta: {
                    count: config?.triggersCount || (config?.triggers || mockTriggers).length
                }
            }
        }, {
            overwriteRoutes: false
        });
    };

    const fetcMockValidateName = (id?: string) => {
        fetchMock.postOnce(actionPostPoliciesValidateName({
            body: 'foo',
            id
        }).endpoint, {
            status: 200
        });
    };

    const fetchMockValidateCondition = () => {
        fetchMock.postOnce(actionPostPoliciesValidate({
            body: undefined
        }).endpoint, {
            status: 200
        });
    };

    const fetchMockSavePolicy = (edit: boolean, updatePolicy: Partial<ServerPolicyRequest>) => {
        const policy = { ...mockPolicy, ...updatePolicy };

        if (edit) {
            fetchMock.putOnce(actionPutPoliciesByPolicyId({
                policyId: policy.id,
                body: policy
            }).endpoint, {
                status: 200,
                body: policy
            });
        } else {
            fetchMock.postOnce(actionPostPolicies({
                alsoStore: true,
                body: mockPolicy
            }).endpoint, {
                status: 200,
                body: policy
            });
        }
    };

    const fetchMockDelete = () => {
        fetchMock.deleteOnce(actionDeletePoliciesIds({
            body: []
        }).endpoint, {
            status: 200,
            body: [ 'foo' ]
        });
    };

    const fetchMockChangeStatus = (enabled: boolean) => {
        fetchMock.postOnce(actionPostPoliciesIdsEnabled({
            enabled
        }).endpoint, {
            body: []
        });
    };

    it('Refuses to show data if rbac.readAll is false', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                },
                appContext: {
                    rbac: {
                        canWriteAll: true,
                        canReadAll: false
                    },
                    userSettings: {
                        isSubscribedForNotifications: false,
                        refresh: () => '',
                        settings: undefined
                    }
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText('You do not have access to Policies')).toBeVisible();
    });

    it('Renders policy data', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText('Not arch x86_64', {
            selector: 'h1'
        })).toBeVisible();
        expect(screen.getByText('Enabled')).toBeVisible();
        expect(screen.queryAllByText('foo-bar').length).toBe(2);
        expect(screen.queryAllByText('random host').length).toBe(2);
    });

    it('Shows empty state when policy is not found ', async () => {
        fetchMockSetup({
            policyStatus: 404,
            policyIsUndefined: true
        });
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText(/Policy not found/i)).toBeVisible();
    });

    it('Shows error state when policy is has status different than 200 or 404 ', async () => {
        fetchMockSetup({
            policyStatus: 500,
            policy: {
                msg: 'this looks bad'
            }
        });
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText(/this looks bad/i)).toBeVisible();
    });

    it('Shows error state when policy is has status different than 200 or 404, show status when no error msg', async () => {
        fetchMockSetup({
            policyStatus: 500,
            policy: {}
        });
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText(/code: 500/i)).toBeVisible();
    });

    it('On the error state, clicking on the button retries the query', async () => {
        fetchMockSetup({
            policyStatus: 500,
            policy: {}
        });
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        fetchMockSetup({
            policyStatus: 200
        });
        userEvent.click(screen.getByRole('button'));

        await waitForAsyncEvents();
        expect(screen.getAllByText(/Not arch x86/i)).toBeTruthy();
    });

    it('Click on edit brings up edit wizard ', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/edit/i));

        await waitForAsyncEvents();
        expect(screen.getByText(/Edit a policy/i)).toBeVisible();
    });

    it('Click on duplicate brings up create wizard ', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/duplicate/i));

        await waitForAsyncEvents();
        expect(screen.getByText(/Create a policy/i)).toBeVisible();
        expect(screen.getByDisplayValue(/Copy of Not arch x86_64/i)).toBeVisible();
    });

    it('Duplicates navigates to the new policy url ', async () => {
        fetchMockSetup();
        fetcMockValidateName(undefined);
        fetchMockValidateCondition();
        fetchMockSavePolicy(false, {
            id: 'bar-123'
        });
        fetchMockSetup({
            policyId: 'bar-123'
        });

        const getLocation = jest.fn();

        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                },
                getLocation
            })
        });

        await waitForAsyncEvents();
        expect(getLocation().pathname).toEqual(linkTo.policyDetail('foo'));
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/duplicate/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Validate/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Finish/i));
        await waitForAsyncEvents();
        expect(getLocation().pathname).toEqual(linkTo.policyDetail('bar-123'));
    });

    it('Edits updates the policy with the new values ', async () => {
        fetchMockSetup();
        fetcMockValidateName('foo');
        fetchMockValidateCondition();
        fetchMockSavePolicy(true, {
            name: 'my new name'
        });

        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/edit/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Next/i));
        await waitForAsyncEvents();
        userEvent.click(screen.getByText(/Finish/i));
        await waitForAsyncEvents();
        expect(screen.getAllByText('my new name')).toBeTruthy();
    });

    it('Click on delete brings up the delete dialog ', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/delete/i));

        await waitForAsyncEvents();
        expect(screen.getByText(/Do you want to delete the policy/i)).toBeVisible();
    });

    it('Delete dialog can be closed ', async () => {
        fetchMockSetup();
        render(<PolicyDetail/>, {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/delete/i));

        await waitForAsyncEvents();
        userEvent.click(screen.getByText('Cancel'));

        await waitForAsyncEvents();
        expect(screen.queryByText(/Do you want to delete the policy/i)).toBeFalsy();
    });

    it('When policy is deleted, navigates to list page', async () => {
        fetchMockSetup();
        fetchMockDelete();

        const getLocation = jest.fn();
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                },
                getLocation
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/delete/i));

        await waitForAsyncEvents();
        userEvent.click(screen.getByText('Delete'));

        await waitForAsyncEvents();
        expect(getLocation().pathname).toEqual(linkTo.listPage());
    });
    it('When policy is disabled, it updates the enabled status in the page', async () => {
        fetchMockSetup();
        fetchMockChangeStatus(false);
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/disable/i));

        await waitForAsyncEvents();
        expect(screen.getByText('Disabled')).toBeVisible();
    });

    it('When policy is enabled, it updates the enabled status in the page to disabled', async () => {
        fetchMockSetup({
            policy: { ...mockPolicy, isEnabled: false }
        });
        fetchMockChangeStatus(true);
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        userEvent.click(screen.getByTestId('policy-detail-actions-button'));
        userEvent.click(screen.getByText(/enable/i));

        await waitForAsyncEvents();
        expect(screen.getByText('Enabled')).toBeVisible();
    });

    it('Export button downloads a file with the current time', async () => {
        fetchMockSetup();
        const dateNow = jest.spyOn(Date, 'now');
        dateNow.mockImplementation(() => new Date(2020, 10, 5).getTime());

        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        fetchMockSetup({
            triggersCount: 430,
            triggerLimit: 200,
            triggerOffset: 0
        });
        fetchMockSetup({
            triggersCount: 430,
            triggerLimit: 200,
            triggerOffset: 200
        });
        fetchMockSetup({
            triggersCount: 430,
            triggerLimit: 200,
            triggerOffset: 400
        });

        userEvent.click(getByRole(screen.getByTestId('trigger-toolbar-export-container'), 'button'));
        userEvent.click(screen.getByText(/Export to JSON/i));

        await waitForAsyncEvents();
        expect(inBrowserDownload.mock.calls[0][1]).toEqual('policy-foo-triggers-2020-05-11.json');
        dateNow.mockRestore();
    });

    it('Export button is not found when no triggers', async () => {
        fetchMockSetup({
            triggers: []
        });
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.queryByTestId('trigger-toolbar-export-container')).toBeFalsy();
    });

    it('Trigger history does not show loading when not loading', async () => {
        fetchMockSetup({
            triggerLoading: false
        });
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.queryByText('Loading Triggers')).toBeFalsy(); // Mocked the loading element
    });

    it('Trigger history shows loading when loading it', async () => {
        fetchMockSetup({
            triggerLoading: true
        });
        render((
            <>
                <PolicyDetail/>
            </>
        ), {
            wrapper: getConfiguredAppWrapper({
                router: {
                    initialEntries: [ linkTo.policyDetail('foo') ]
                },
                route: {
                    path: linkTo.policyDetail(':policyId')
                }
            })
        });

        await waitForAsyncEvents();
        expect(screen.getByText('Loading Triggers')).toBeVisible(); // Mocked the loading element
    });
});

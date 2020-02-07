import * as React from 'react';
import { Form } from '@patternfly/react-core';

import { WizardStepExtended } from '../PolicyWizardTypes';
import { PolicyFormActions } from '../../../schemas/CreatePolicy/PolicySchema';
import { FieldArray, FieldArrayRenderProps } from 'formik';
import { ActionsForm } from '../ActionsForm';

const ActionsStep = () => {
    return (
        <Form>
            <FieldArray name="actions">
                { (helpers: FieldArrayRenderProps) => {
                    return <ActionsForm id="actions" name="actions" actions={ helpers.form.values.actions } arrayHelpers={ helpers }/>;
                } }
            </FieldArray>
        </Form>
    );
};

export const createActionsStep: (stepOverrides?: Partial<WizardStepExtended>) => WizardStepExtended = (stepOverrides) => ({
    name: 'Actions',
    component: <ActionsStep/>,
    validationSchema: PolicyFormActions,
    ...stepOverrides
});

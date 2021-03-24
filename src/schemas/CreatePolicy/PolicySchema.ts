import { assertNever } from 'assert-never';
import * as Yup from 'yup';
import { ValidationError } from 'yup';

import { Action, ActionType } from '../../types/Policy/Actions';
import { isAction } from '../../types/Policy/Actions/Action';
import { maxPolicyNameLength } from '../../types/Policy/Policy';
import { ActionNotificationSchema, ActionSchema } from './Actions';

const ActionSchemaSelector = (action: Action | any): Yup.Schema<any> => {
    if (action?.type && isAction(action)) {
        const type = action.type;
        switch (type) {
            case ActionType.NOTIFICATION:
                return ActionNotificationSchema;
            default:
                assertNever(type);
        }
    }

    return ActionSchema;
};

const oneActionOf =
    (type: ActionType, typeDescription: string, value: string):
        [string, string, (actions: Action[] | undefined) => ValidationError | true] => {
        const message = `Only one ${typeDescription} action is allowed`;
        return [
            `one-${value}`,
            message,
            (actions: (Action | undefined)[] | undefined) => {
                const indexes = actions && actions.reduce<number[]>((indexes, action, index) => {
                    if (action?.type === type) {
                        return indexes.concat([ index ]);
                    }

                    return indexes;
                }, []);
                if (!indexes || indexes.length <= 1) {
                    return true;
                }

                const validationError = new ValidationError(
                    '',
                    '',
                    'actions',
                    ''
                );

                validationError.inner = indexes.map(index => new ValidationError(
                    message,
                    value,
                    `actions.${index}.type`
                ));
                return validationError;
            }
        ];
    };

export const PolicyFormDetails = Yup.object().shape({
    description: Yup.string().notRequired().trim(),
    isEnabled: Yup.boolean().notRequired(),
    name: Yup.string().required('Write a name for this Policy').max(maxPolicyNameLength).trim()
});

export const PolicyFormActions = Yup.object().shape({
    actions: Yup.array(Yup.lazy(ActionSchemaSelector))
    .test(...oneActionOf(ActionType.NOTIFICATION, 'Hook', 'notification'))
});

export const PolicyFormConditions = Yup.object().shape({
    conditions: Yup.string().required('Enter a condition for the policy.').trim()
});

export const PolicyFormSchema = Yup.object().concat(PolicyFormDetails).concat(PolicyFormActions).concat(PolicyFormConditions);

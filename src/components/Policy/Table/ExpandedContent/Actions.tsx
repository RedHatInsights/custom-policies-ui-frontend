import * as React from 'react';
import {
    Divider,
    Stack,
    StackItem,
    Text,
    Title
} from '@patternfly/react-core';
import { style } from 'typestyle';
import { Action, ActionType } from '../../../../types/Policy/Actions';
import { Messages } from '../../../../properties/Messages';
import { assertNever, join } from 'common-code-ui';
import { SVGIconProps } from '@patternfly/react-icons/dist/js/createIcon';
import { ActionEmailIcon, ActionWebhookIcon } from '../../ActionIcons';

interface ActionsProps {
    actions: Action[];
}

const titleClassName = style({
    marginBottom: 5
});

const titleActionClassName = style({
    marginBottom: 5,
    marginLeft: 10,
    display: 'inline'
});

const wrapperClassName = style({
    marginBottom: 10,
    marginTop: 10,
    marginLeft: 10
});

const actionContentWrapperClassName = style({
    marginLeft: 25,
    marginBottom: 8,
    marginTop: 8
});

const ActionWrapper: React.FunctionComponent<{
    title: string;
    icon: React.ElementType<SVGIconProps>;
}> = (props) => {
    return (
        <StackItem className={ wrapperClassName }>
            { <props.icon/> }
            <Title headingLevel="h2" className={ titleActionClassName } size="md">{ props.title }</Title>
            { props.children &&
                <div className={ actionContentWrapperClassName }>
                    { props.children }
                </div>
            }
        </StackItem>
    );
};

const getActions = (actions: Action[]) => {
    const elements: React.ReactElement<React.ReactFragment>[] = [];
    for (const index in actions) {
        const action = actions[index];
        switch (action.type) {
            case ActionType.EMAIL:
                elements.push((
                    <React.Fragment key={ index }>
                        <ActionWrapper title="Send Email" icon={ ActionEmailIcon }/>
                    </React.Fragment>
                ));
                break;
            case ActionType.WEBHOOK:
                elements.push((
                    <React.Fragment key={ index }>
                        <ActionWrapper title="Send to Hook" icon={ ActionWebhookIcon }/>
                    </React.Fragment>
                ));
                break;
            default:
                assertNever(action);
        }
    }

    return join(elements, Divider);
};

export const Actions: React.FunctionComponent<ActionsProps> = (props) => {

    return (
        <>
            <Stack>
                <StackItem>
                    <Title headingLevel="h2" className={ titleClassName } size="md">{ Messages.components.actions.title }</Title>
                </StackItem>
                {
                    props.actions.length === 0 ? (
                        <Text> { Messages.components.actions.noActions } </Text>
                    ) : getActions(props.actions)
                }
            </Stack>
        </>
    );
};

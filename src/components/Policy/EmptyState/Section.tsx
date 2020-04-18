import * as React from 'react';
import { Button, EmptyState as EmptyStatePf, EmptyStateBody, EmptyStateIcon, EmptyStateVariant, Title } from '@patternfly/react-core';
import { PlusCircleIcon } from '@patternfly/react-icons';
import { style } from 'typestyle';
import { calc } from 'csx';
import { IconType } from '@patternfly/react-icons/dist/js/createIcon';
import { Spacer } from '../../../utils/Spacer';

const emptyStateClassName = style({
    paddingTop: calc(`${ Spacer.XL_3 }px - var(--pf-c-page__main-section--PaddingTop)`)
});

export interface EmptyStateSectionProps {
    icon?: IconType;
    iconColor?: string;
    title: string;
    content: React.ReactNode;
    action?: () => void;
    actionLabel?: string;
    className?: string;
}

export const EmptyStateSection: React.FunctionComponent<EmptyStateSectionProps> = (props) => (
    <EmptyStatePf variant={ EmptyStateVariant.full } className={ `${emptyStateClassName} ${props.className ? props.className : ''} ` }>
        { props.icon && <EmptyStateIcon icon={ PlusCircleIcon } color={ props.iconColor } /> }
        <Title headingLevel="h5" size="lg">
            { props.title }
        </Title>
        <EmptyStateBody>
            { props.content }
        </EmptyStateBody>
        { props.actionLabel && (
            <Button variant="primary" onClick={ props.action } isDisabled={ !props.action } >{ props.actionLabel }</Button>
        ) }
    </EmptyStatePf>
);

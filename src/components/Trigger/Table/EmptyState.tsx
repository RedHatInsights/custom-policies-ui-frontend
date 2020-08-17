import * as React from 'react';
import { EmptyStateSection } from '../../Policy/EmptyState/Section';
import { Messages } from '../../../properties/Messages';

export const TriggerTableEmptyState: React.FunctionComponent = () => {
    return <EmptyStateSection
        ouiaId="trigger-table-empty-state"
        title={ Messages.tables.trigger.emptyState.notFound.title }
        content={ Messages.tables.trigger.emptyState.notFound.content }
    />;
};

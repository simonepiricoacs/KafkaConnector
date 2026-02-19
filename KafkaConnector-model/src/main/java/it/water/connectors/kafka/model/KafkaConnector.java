package it.water.connectors.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;
import it.water.core.api.entity.owned.OwnedResource;
import it.water.core.api.permission.ProtectedEntity;
import it.water.core.api.service.rest.WaterJsonView;
import it.water.core.permission.action.CrudActions;
import it.water.core.permission.annotations.AccessControl;
import it.water.core.permission.annotations.DefaultRoleAccess;
import it.water.core.validation.annotations.NoMalitiusCode;
import it.water.core.validation.annotations.NotNullOnPersist;
import it.water.repository.jpa.model.AbstractJpaEntity;
import jakarta.persistence.Access;
import jakarta.persistence.AccessType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.UniqueConstraint;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * KafkaConnector entity persisted by Water repository layer.
 */
@Entity
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"name"}))
@Access(AccessType.FIELD)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@RequiredArgsConstructor
@Getter
@Setter(AccessLevel.PROTECTED)
@ToString
@EqualsAndHashCode(of = {"name"}, callSuper = true)
@AccessControl(
    availableActions = {CrudActions.SAVE, CrudActions.UPDATE, CrudActions.FIND, CrudActions.FIND_ALL, CrudActions.REMOVE},
    rolesPermissions = {
        @DefaultRoleAccess(roleName = KafkaConnector.DEFAULT_MANAGER_ROLE, actions = {CrudActions.SAVE, CrudActions.UPDATE, CrudActions.FIND, CrudActions.FIND_ALL, CrudActions.REMOVE}),
        @DefaultRoleAccess(roleName = KafkaConnector.DEFAULT_VIEWER_ROLE, actions = {CrudActions.FIND, CrudActions.FIND_ALL}),
        @DefaultRoleAccess(roleName = KafkaConnector.DEFAULT_EDITOR_ROLE, actions = {CrudActions.SAVE, CrudActions.UPDATE, CrudActions.FIND, CrudActions.FIND_ALL})
    }
)
public class KafkaConnector extends AbstractJpaEntity implements ProtectedEntity, OwnedResource {

    public static final String DEFAULT_MANAGER_ROLE = "kafkaConnectorManager";
    public static final String DEFAULT_VIEWER_ROLE = "kafkaConnectorViewer";
    public static final String DEFAULT_EDITOR_ROLE = "kafkaConnectorEditor";

    @NoMalitiusCode
    @NotNull
    @NotNullOnPersist
    @NonNull
    @Setter
    @JsonView(WaterJsonView.Extended.class)
    @Column(unique = true, nullable = false, length = 255)
    private String name;

    @NoMalitiusCode
    @NotNull
    @NotNullOnPersist
    @Setter
    @JsonView(WaterJsonView.Extended.class)
    @Column(nullable = false, length = 32)
    private String type = "source";

    // Runtime payload from Kafka Connect API, not persisted in local DB.
    @Transient
    @Setter
    @JsonView(WaterJsonView.Extended.class)
    private ConnectorConfig config;

    // Runtime payload from Kafka Connect API, not persisted in local DB.
    @Transient
    @Setter
    @JsonView(WaterJsonView.Extended.class)
    private ConnectorTask[] tasks;

    @Setter
    @JsonIgnore
    @JsonView(WaterJsonView.Extended.class)
    private Long ownerUserId;
}

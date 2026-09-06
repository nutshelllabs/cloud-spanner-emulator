@{OPTIMIZER_VERSION=8,FORCE_JOIN_ORDER=true}

WITH
item_context AS (
  SELECT
    g.*
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (item_head:ArtifactHead
        WHERE item_head.workspace_id = @workspace_id
          AND item_head.type = @item_type
          AND item_head.artifact_id >= @artifact_id_start
          AND item_head.artifact_id < @artifact_id_end)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (item:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (item_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[member_of:RelatedArtifact WHERE member_of.relation_type = 'MEMBER_OF']->
      @{JOIN_METHOD=APPLY_JOIN}
      (group_head:ArtifactHead {workspace_id: @workspace_id, type: @group_type})

    RETURN
      item.artifact_id AS item_artifact_id,
      item.revision_version AS item_revision_version,
      group_head.artifact_id AS group_artifact_id
  ) g
),

group_config AS (
  SELECT
    g.*
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (group_head:ArtifactHead {workspace_id: @workspace_id, type: @group_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (group_node:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (group_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[has_configuration:RelatedArtifact WHERE has_configuration.relation_type = 'HAS']->
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head:ArtifactHead {workspace_id: @workspace_id, type: @configuration_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[has_batch:RelatedArtifact WHERE has_batch.relation_type = 'HAS_BATCH']->
      @{JOIN_METHOD=APPLY_JOIN}
      (batch_head:ArtifactHead {workspace_id: @workspace_id, type: @batch_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (batch:Artifact)

    RETURN
      group_node.artifact_id AS group_artifact_id,
      group_node.revision_version AS group_revision_version,
      configuration.artifact_id AS configuration_artifact_id,
      configuration.revision_version AS configuration_revision_version,
      batch.artifact_id AS batch_artifact_id,
      batch.revision_version AS batch_revision_version
  ) g
),

candidate_context AS (
  SELECT
    CONCAT(item.item_artifact_id, '|', config.batch_artifact_id) AS candidate_key,
    item.item_artifact_id,
    item.item_revision_version,
    config.group_artifact_id,
    config.group_revision_version,
    config.configuration_artifact_id,
    config.configuration_revision_version,
    config.batch_artifact_id,
    config.batch_revision_version
  FROM item_context item
  JOIN group_config config
    ON config.group_artifact_id = item.group_artifact_id
),

candidate_inputs AS (
  SELECT candidate_key, 'item' AS role, item_artifact_id AS artifact_id, item_revision_version AS revision_version, TRUE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'group' AS role, group_artifact_id AS artifact_id, group_revision_version AS revision_version, FALSE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'configuration' AS role, configuration_artifact_id AS artifact_id, configuration_revision_version AS revision_version, FALSE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'batch' AS role, batch_artifact_id AS artifact_id, batch_revision_version AS revision_version, FALSE AS strong FROM candidate_context
),

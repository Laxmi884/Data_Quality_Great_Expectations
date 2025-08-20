"""Framework for data quality validation using Great Expectations."""

from typing import Optional, List, Dict, Any, Union
import os

import great_expectations as gx
from great_expectations.core import RunIdentifier
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.checkpoint.actions import UpdateDataDocsAction
from great_expectations.core.expectation_suite import ExpectationSuite
import os




class SagenDataQuality:
    """A class to manage data quality validation using Great Expectations.

    This class provides a high-level interface for setting up and running data quality
    validations using the Great Expectations framework. It handles data sources,
    expectations, validations, and checkpoints.

    Args:
        df: The pandas DataFrame to validate.
        mode (str, optional): The context mode. Defaults to "file".
        project_root_dir (str, optional): The root directory for the Great Expectations project.
            Defaults to current working directory.

    Attributes:
        df: The input DataFrame.
        context: The Great Expectations context.
        batch_parameters: Parameters for batch processing.
    """

    def __init__(self, df, mode: str = "file", project_root_dir: Optional[str] = None) -> None:
        if df is None:
            raise ValueError("DataFrame cannot be None")
        
        self.df = df
        self.project_root_dir = project_root_dir if project_root_dir else os.getcwd()
        self.context = self._initialize_context(mode=mode, project_root_dir=self.project_root_dir)
        self.batch_parameters = {"dataframe": df}

    def _initialize_context(self, mode: str = "file", project_root_dir: Optional[str] = None) -> object:
        """Initialize the Great Expectations context.

        Args:
            mode: The context mode to use.
            project_root_dir: The root directory for the Great Expectations project.

        Returns:
            DataContext: Initialized Great Expectations context.

        Raises:
            ValueError: If context initialization fails.
        """
        try:
            if project_root_dir is None:
                project_root_dir = os.getcwd()
            return gx.get_context(mode=mode, project_root_dir=project_root_dir)
        except Exception as e:
            raise ValueError(f"Failed to initialize Great Expectations context: {str(e)}")
    

    def set_data_source(self, data_source_name: str, data_frame_type: str = "pandas") -> object:
        """Create and add a new data source to the context.

        Args:
            data_source_name: Name of the data source to create.
            data_frame_type: Type of the DataFrame. Currently only "pandas" is supported.

        Returns:
            The created data source object.

        Raises:
            ValueError: If data_source_name is empty or if data_frame_type is not supported.
        """
        if not data_source_name:
            raise ValueError("data_source_name cannot be empty")
        
        if data_frame_type != "pandas":
            raise ValueError("Only pandas DataFrame type is currently supported")
        
        try:
            return self.context.data_sources.add_pandas(name=data_source_name)
        except Exception as e:
            raise ValueError(f"Failed to create data source '{data_source_name}': {str(e)}")
    
    def get_data_source(self, data_source_name: str) -> object:
        """Retrieve an existing data source from the context.

        Args:
            data_source_name: Name of the data source to retrieve.

        Returns:
            The requested data source object.

        Raises:
            ValueError: If data_source_name is empty or if the data source doesn't exist.
        """
        if not data_source_name:
            raise ValueError("data_source_name cannot be empty")
        
        try:
            return self.context.data_sources.get(data_source_name)
        except Exception as e:
            raise ValueError(f"Data source '{data_source_name}' not found: {str(e)}")

    def set_data_asset(self, data_source: object, data_asset_name: str, data_source_name: Optional[str] = None) -> object:
        """Create and add a new data asset to a data source.

        Args:
            data_source: The data source to add the asset to.
            data_asset_name: Name of the data asset to create.
            data_source_name: Optional Name of the data source. Defaults to None. 

        Returns:
            The created data asset object.

        Raises:
            ValueError: If data_source is None or data_asset_name is empty.
        """
        if data_source is None:
            if data_source_name:
                data_source = self.get_data_source(data_source_name)
            else:
                raise ValueError("Please provide either data_source object or data_source_name")
        if not data_asset_name:
            raise ValueError("data_asset_name cannot be empty")
        
        try:
            return data_source.add_dataframe_asset(name=data_asset_name)
        except Exception as e:
            raise ValueError(f"Failed to create data asset '{data_asset_name}': {str(e)}")
    
    def get_data_asset(self,  data_asset_name: str, data_source: object = None,data_source_name: Optional[str] = None) -> object:
        """Retrieve an existing data asset from a data source.

        Args:
            
            data_asset_name: Name of the data asset to retrieve.
            data_source: Optional The data source containing the asset.
            data_source_name: Optional Name of the data source. Defaults to None.

        Returns:
            The requested data asset object.

        Raises:
            ValueError: If data_source is None, data_asset_name is empty,
                      or if the asset doesn't exist.
        """
        if data_source is None:
            if data_source_name:
                data_source = self.get_data_source(data_source_name)
            else:
                raise ValueError("Please provide either data_source object or data_source_name")
        if not data_asset_name:
            raise ValueError("data_asset_name cannot be empty")
        
        try:
            return data_source.get_asset(data_asset_name)
        except Exception as e:
            raise ValueError(f"Data asset '{data_asset_name}' not found: {str(e)}")
    

    def set_batch_definition(self,  batch_definition_name: str, data_asset: object = None ,data_asset_name: Optional[str] = None,data_source_name: Optional[str] = None) -> object:
        """Create a new batch definition for a data asset. You need to provide batch definition name and 
        data asset object or data asset name and data source name so that it can automatically get data asset object.

        Args:
            data_asset: The data asset to create the batch definition for.
            batch_definition_name: Name of the batch definition to create.
            data_asset_name: Optional Name of the data asset. Defaults to None.
            data_source_name: Optional Name of the data source. Defaults to None.

        Returns:
            The created batch definition object.

        Raises:
            ValueError: If data_asset is None or batch_definition_name is empty.
        """
        if data_asset is None:
            if data_asset_name and data_source_name:
                data_asset = self.get_data_asset(data_asset_name=data_asset_name,data_source_name=data_source_name)
            else:
                raise ValueError("Please provide either data_asset object or data_asset_name and data_source_name")
        if not batch_definition_name:
            raise ValueError("batch_definition_name cannot be empty")
        
        try:
            return data_asset.add_batch_definition_whole_dataframe(name=batch_definition_name)
        except Exception as e:
            raise ValueError(f"Failed to create batch definition '{batch_definition_name}': {str(e)}")
    
    def get_batch_definition(
        self, 
        batch_definition_name: str,
        data_source_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_asset: Optional[object] = None
    ) -> object:
        """Retrieve an existing batch definition.

        This method can retrieve a batch definition either directly from a data asset
        or by looking it up using the data source and asset names.

        Args:
            batch_definition_name: Name of the batch definition to retrieve.
            data_source_name: Optional name of the data source.
            data_asset_name: Optional name of the data asset.
            data_asset: Optional data asset object.

        Returns:
            The requested batch definition object.

        Raises:
            ValueError: If neither data_asset nor (data_source_name, data_asset_name) are provided,
                      or if the batch definition cannot be found.
        """
        if not batch_definition_name:
            raise ValueError("batch_definition_name cannot be empty")

        try:
            if data_asset is not None:
                return data_asset.get_batch_definition(batch_definition_name)

            if data_source_name and data_asset_name:
                data_source = self.get_data_source(data_source_name)
                data_asset = self.get_data_asset(data_asset_name=data_asset_name, data_source_name=data_source_name)
                return data_asset.get_batch_definition(batch_definition_name)

            raise ValueError(
                "You must provide either a 'data_asset' object or both 'data_source_name' "
                "and 'data_asset_name'."
            )
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve batch definition '{batch_definition_name}': {str(e)}"
            )
    

    def get_data_batch(
        self,
        batch_definition: Optional[BatchDefinition] = None,
        batch_definition_name: Optional[str] = None,
        data_source_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_asset: Optional[object] = None
    ) -> object:
        """Retrieve a batch of data using the provided parameters.

        This method can retrieve a batch either directly using a batch definition
        or by looking up the batch definition using the provided names.

        Args:
            batch_definition: Optional batch definition object.
            batch_definition_name: Optional name of the batch definition.
            data_source_name: Optional name of the data source.
            data_asset_name: Optional name of the data asset.
            data_asset: Optional data asset object.

        Returns:
            A batch object containing the requested data.

        Raises:
            ValueError: If neither batch_definition nor the combination of names
                      (batch_definition_name, data_source_name, data_asset_name)
                      is provided, or if the batch cannot be retrieved.
        """
        try:
            if batch_definition is not None:
                return batch_definition.get_batch(batch_parameters=self.batch_parameters)
            
            if all([batch_definition_name, data_source_name, data_asset_name]):
                batch_definition = self.get_batch_definition(
                    batch_definition_name=batch_definition_name,
                    data_source_name=data_source_name,
                    data_asset_name=data_asset_name
                )
                return batch_definition.get_batch(batch_parameters=self.batch_parameters)
            
            raise ValueError(
                "You must provide either a 'batch_definition' or the combination of "
                "'batch_definition_name', 'data_source_name', and 'data_asset_name'."
            )
        except Exception as e:
            raise ValueError(f"Failed to retrieve batch: {str(e)}")
    

    def create_expectation_suite(self, suite_name: str) -> ExpectationSuite:
        """Create a new expectation suite.

        Args:
            suite_name: Name of the expectation suite to create.

        Returns:
            The created expectation suite object.

        Raises:
            ValueError: If suite_name is empty or if creation fails.
        """
        if not suite_name:
            raise ValueError("suite_name cannot be empty")
        
        try:
            suite = gx.ExpectationSuite(name=suite_name)
            self.context.suites.add(suite)
            return suite
        except Exception as e:
            raise ValueError(f"Failed to create expectation suite '{suite_name}': {str(e)}")

    def get_expectation_suite(self, suite_name: str) -> ExpectationSuite:
        """Retrieve an existing expectation suite.

        Args:
            suite_name: Name of the expectation suite to retrieve.

        Returns:
            The requested expectation suite object.

        Raises:
            ValueError: If suite_name is empty or if the suite doesn't exist.
        """
        if not suite_name:
            raise ValueError("suite_name cannot be empty")
        
        try:
            return self.context.suites.get(suite_name)
        except Exception as e:
            raise ValueError(f"Failed to retrieve expectation suite '{suite_name}': {str(e)}")
    
    def add_expectation(
        self,
        expectation: Any,
        suite: Optional[ExpectationSuite] = None,
        suite_name: Optional[str] = None
    ) -> ExpectationSuite:
        """Add an expectation to a suite.

        Args:
            expectation: The expectation to add.
            suite: Optional expectation suite object.
            suite_name: Optional name of the expectation suite.

        Returns:
            The updated expectation suite object.

        Raises:
            ValueError: If neither suite nor suite_name is provided,
                      or if the expectation cannot be added.
        """
        if expectation is None:
            raise ValueError("expectation cannot be None")
        if suite is None and not suite_name:
            raise ValueError("Either 'suite' or 'suite_name' must be provided")

        try:
            target_suite = suite if suite is not None else self.get_expectation_suite(suite_name)
            target_suite.add_expectation(expectation)
            target_suite.save()
            return target_suite
        except Exception as e:
            raise ValueError(f"Failed to add expectation: {str(e)}")
    
    
    def create_validation_definition(
        self,
        validation_definition_name: str,
        suite: ExpectationSuite,
        batch_definition: Optional[BatchDefinition] = None,
        data_source_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_asset: Optional[object] = None
    ) -> Any:
        """Create a new validation definition.

        Args:
            validation_definition_name: Name of the validation definition.
            suite: The expectation suite to use.
            batch_definition: Optional batch definition.
            data_source_name: Optional name of the data source.
            data_asset_name: Optional name of the data asset.
            data_asset: Optional data asset object.

        Returns:
            The created validation definition object.

        Raises:
            ValueError: If required parameters are missing or if creation fails.
        """
        if not validation_definition_name:
            raise ValueError("validation_definition_name cannot be empty")
        if suite is None:
            raise ValueError("suite cannot be None")

        try:
            if batch_definition is None:
                if data_asset is not None:
                    batch_definition = data_asset.get_batch_definition()
                elif data_source_name and data_asset_name:
                    data_source = self.get_data_source(data_source_name)
                    data_asset = self.get_data_asset(data_source, data_asset_name)
                    batch_definition = data_asset.get_batch_definition()
                else:
                    raise ValueError(
                        "You must provide either a 'batch_definition', a 'data_asset', "
                        "or both 'data_source_name' and 'data_asset_name'."
                    )
            else:
                validation_definition_name = gx.ValidationDefinition(
                    data = batch_definition,
                    suite = suite,
                    name = validation_definition_name
                )

            return self.context.validation_definitions.add(
                validation_definition_name
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create validation definition '{validation_definition_name}': {str(e)}"
            )

    def run_validation(self, validation_definition_name: str) -> Any:
        """Run a validation using the specified validation definition.

        Args:
            validation_definition_name: Name of the validation definition to run.

        Returns:
            The validation results.

        Raises:
            ValueError: If validation_definition_name is empty or if validation fails.
        """
        if not validation_definition_name:
            raise ValueError("validation_definition_name cannot be empty")
        
        try:
            validation_definition = self.context.validation_definitions.get(
                name=validation_definition_name
            )
            return validation_definition.run(batch_parameters=self.batch_parameters)
        except Exception as e:
            raise ValueError(f"Failed to run validation: {str(e)}")

    def get_validation_definition(self, validation_definition_name: str) -> Any:
        """Retrieve an existing validation definition.

        Args:
            validation_definition_name: Name of the validation definition to retrieve.

        Returns:
            The requested validation definition object.

        Raises:
            ValueError: If validation_definition_name is empty or if retrieval fails.
        """
        if not validation_definition_name:
            raise ValueError("validation_definition_name cannot be empty")
        
        try:
            return self.context.validation_definitions.get(name=validation_definition_name)
        except Exception as e:
            raise ValueError(
                f"Failed to retrieve validation definition '{validation_definition_name}': {str(e)}"
            )
    
    def create_action_list(
        self,
        action_list_name: str,
        actions: Optional[List[Any]] = None
    ) -> Any:
        """Create a new action list.

        Args:
            action_list_name: Name of the action list to create.
            actions: Optional list of actions. If None, a default UpdateDataDocsAction
                    will be created.

        Returns:
            The created action list object.

        Raises:
            ValueError: If action_list_name is empty or if creation fails.
        """
        if not action_list_name:
            raise ValueError("action_list_name cannot be empty")

        try:
            if actions is None:
                if action_list_name == "delq_history_checkpoint_dev_actions":
                    actions = [
                        UpdateDataDocsAction(name=f"Update Data Docs for {action_list_name}", site_names=["local_site1"])
                    ]
                else:
                    actions = [
                        UpdateDataDocsAction(name=f"Update Data Docs for {action_list_name}", site_names=["local_site2"])
                    ]

            action_list = actions
            return action_list
        except Exception as e:
            raise ValueError(f"Failed to create action list '{action_list_name}': {str(e)}")

    def create_checkpoint(
        self,
        checkpoint_name: str,
        validation_definition: Any,
        action_list: Optional[Any] = None,
        result_format: str = "COMPLETE"
    ) -> Any:
        """Create a new checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint to create.
            validation_definition: The validation definition to use.
            action_list: Optional action list object.
            result_format: Format for validation results. Defaults to "COMPLETE".

        Returns:
            The created checkpoint object.

        Raises:
            ValueError: If required parameters are missing or if creation fails.
        """
        if not checkpoint_name:
            raise ValueError("checkpoint_name cannot be empty")
        if validation_definition is None:
            raise ValueError("validation_definition cannot be None")

        try:
            if action_list is None:
                action_list = self.create_action_list(
                    action_list_name=f"{checkpoint_name}_actions"
                )

            checkpoint = gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=[validation_definition],
                actions=action_list,
                result_format={"result_format": result_format}
            )
            self.context.checkpoints.add(checkpoint)
            return checkpoint
        except Exception as e:
            raise ValueError(f"Failed to create checkpoint '{checkpoint_name}': {str(e)}")

    def run_checkpoint(
        self,
        checkpoint_name: str,
        run_id: Optional[str] = None
    ) -> Any:
        """Run a checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint to run.
            run_id: Optional identifier for the run. If None, a default one will be created.

        Returns:
            The results of the checkpoint run.

        Raises:
            ValueError: If checkpoint_name is empty or if the run fails.
        """
        if not checkpoint_name:
            raise ValueError("checkpoint_name cannot be empty")

        try:
            run_identifier = RunIdentifier(
                run_name=run_id if run_id else f"run_{checkpoint_name}"
            )

            checkpoint = self.context.checkpoints.get(checkpoint_name)
            results = checkpoint.run(
                run_id=run_identifier,
                batch_parameters=self.batch_parameters,
                
            )
            
            # Log the result but avoid print statements
            if not results.success:
                print(f"Checkpoint '{checkpoint_name}' validation failed")
            else:
                print(f"Checkpoint '{checkpoint_name}' validation succeeded")
                
            return results
        except Exception as e:
            raise ValueError(f"Failed to run checkpoint '{checkpoint_name}': {str(e)}")

    


    

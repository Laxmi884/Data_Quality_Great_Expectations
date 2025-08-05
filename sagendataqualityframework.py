import pandas as pd
import numpy as np
from great_expectations.core import RunIdentifier
import great_expectations as gx
from great_expectations.checkpoint.actions import UpdateDataDocsAction
import os




class SagenDataQuality:
    def __init__(self,df):
        self.df = df
        self.context = self.getcontext()
        self.batch_parameters = {"dataframe": df}
        

    def getcontext(self,mode="file",project_root_dir=None):
        if project_root_dir is None:
            project_root_dir = os.getcwd()
        context = gx.get_context(mode=mode, project_root_dir=project_root_dir)
        return context
    

    def set_data_source(self,data_source_name,data_frame_type = None):
        if data_frame_type is None:
            data_frame_type = "pandas"
        
        if data_frame_type == "pandas":
            data_source = self.context.data_sources.add_pandas(name=data_source_name)
        return data_source
    
    def get_data_source(self,data_source_name):
        data_source = self.context.data_sources.get(data_source_name)
        return data_source
    

    def set_data_asset(self,data_source,data_asset_name):
        #data_source = self.get_data_source(context,data_source_name)
        data_asset = data_source.add_dataframe_asset(name=data_asset_name)
        return data_asset
    
    def get_data_asset(self,data_source,data_asset_name):
        data_asset = data_source.get_asset(data_asset_name)
        return data_asset
    

    def set_batch_definition(self,data_asset,batch_definition_name):
        batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_definition_name)
        return batch_definition
    
    def get_batch_definition(self, batch_definition_name, data_source_name=None, data_asset_name=None, data_asset=None):
        if data_asset:
            return data_asset.get_batch_definition(batch_definition_name)

        if data_source_name and data_asset_name:
            try:
                data_source = self.get_data_source(data_source_name)
                data_asset = self.get_data_asset(data_source, data_asset_name)
                return data_asset.get_batch_definition(batch_definition_name)
            except Exception as e:
                raise ValueError(f"Error retrieving batch definition from data source '{data_source_name}' and asset '{data_asset_name}': {e}")

        raise ValueError("You must provide either a 'data_asset' object or both 'data_source_name' and 'data_asset_name'.")
    

    def get_data_batch(self,batch_definition= None, batch_definition_name=None, data_source_name=None, data_asset_name=None, data_asset=None):
        try:
            if batch_definition:
                return batch_definition.get_batch(batch_parameters=self.batch_parameters)
            elif batch_definition_name and data_source_name and data_asset_name:
                batch_definition = self.get_batch_definition(batch_definition_name = batch_definition_name, data_source_name = data_source_name, data_asset_name =data_asset_name)
                return batch_definition.get_batch(batch_parameters=self.batch_parameters)
        except Exception as e:
            raise ValueError(f"Error retrieving batch: {e}")
        raise ValueError("You must provide either a 'batch_definition' or 'batch_definition_name', or both 'data_source_name' and 'data_asset_name'.")
    

    def create_expecation_suite(self,suit_name):
        try:
            if suit_name:
                suite = gx.ExpectationSuite(name=suit_name)
                self.context.suites.add(suite)
                return suite
        except Exception as e:
            raise ValueError(f"Error creating expectation suite '{suit_name}': {e}")
        raise ValueError("You must provide a 'suit_name' to create the expectation suite.")
    

    def get_expectation_suite(self, suite_name):
        try:
            suite = self.context.suites.get(suite_name)
            return suite
        except Exception as e:
            raise ValueError(f"Error retrieving expectation suite '{suite_name}': {e}")
        raise ValueError("You must provide a 'suite_name' to retrieve the expectation suite.")
    
    def add_expectation(self,expectation, suite= None, suite_name = None):
        try:
            if suite:
                suite.add_expectation(expectation)
                suite.save()
                return suite
            
            elif suite_name:
                suite = self.get_expectation_suite(suite_name)
                suite.add_expectation(expectation)
                suite.save()
                return suite
        except Exception as e:
            raise ValueError(f"Error adding expectation '{expectation}': {e}")
        raise ValueError("You must provide both ('suite' or 'suite_name') and 'expectation_type' to add an expectation.")
    
    
    def create_validation_definition(
        self,
        validation_definition_name,
        suite,
        batch_definition=None,
        data_source_name=None,
        data_asset_name=None,
        data_asset=None
    ):
        if not validation_definition_name:
            raise ValueError("You must provide a 'validation_definition_name'.")
        if not suite:
            raise ValueError("You must provide an 'expectation suite'.")

        if batch_definition is None:
            if data_asset:
                batch_definition = data_asset.get_batch_definition()
            elif data_source_name and data_asset_name:
                try:
                    data_source = self.get_data_source(data_source_name)
                    data_asset = self.get_data_asset(data_source, data_asset_name)
                    batch_definition = data_asset.get_batch_definition()
                except Exception as e:
                    raise ValueError(f"Error retrieving batch definition: {e}")
            else:
                raise ValueError("You must provide either a 'batch_definition', a 'data_asset', or both 'data_source_name' and 'data_asset_name'.")

        try:
            return self.context.validation_definitions.add(
                name=validation_definition_name,
                expectation_suite=suite,
                data=batch_definition
            )
        except Exception as e:
            raise ValueError(f"Error creating validation definition '{validation_definition_name}': {e}")
        

    def run_validation(self,validation_definition_name):
        try:
            validation_definition = self.context.validation_definitions.get(name=validation_definition_name)
            results = validation_definition.run(batch_parameters=self.batch_parameters)
            return results
        except Exception as e:
            raise ValueError(f"Error running validation: {e}")
        

    def get_validation_definition(self, validation_definition_name):
        try:
            return self.context.validation_definitions.get(name=validation_definition_name)
        except Exception as e:
            raise ValueError(f"Error retrieving validation definition '{validation_definition_name}': {e}")
        raise ValueError("You must provide a 'validation_definition_name' to retrieve the validation definition.")
    
    def create_action_list(self, action_list_name, actions=None):
        if not action_list_name:
            raise ValueError("You must provide an 'action_list_name'.")
        elif  actions is None:
            action_list = [UpdateDataDocsAction(name=f"Update Data Docs for {action_list_name}")
                        ]

        try:
            action_list = gx.ActionList(name=action_list_name, actions=actions)
            self.context.action_lists.add(action_list)
            return action_list
        except Exception as e:
            raise ValueError(f"Error creating action list '{action_list_name}': {e}")
    

    def create_checkpoint(self,checkpoint_name, validation_definition, action_list=None,result_format="COMPLETE"):
        if not checkpoint_name:
            raise ValueError("You must provide a 'checkpoint_name'.")
        if not validation_definition:
            raise ValueError("You must provide a 'validation_definition'.")

        if action_list is None:
            action_list = self.create_action_list(action_list_name=f"{checkpoint_name}_actions")

        try:
            checkpoint = gx.Checkpoint(
                name=checkpoint_name,
                context=self.context,
                validation_definition=validation_definition,
                action_list=action_list,
                result_format= {"result_format": result_format}
            )
            self.context.checkpoints.add(checkpoint)
            return checkpoint
        except Exception as e:
            raise ValueError(f"Error creating checkpoint '{checkpoint_name}': {e}")
        

    def run_checkpoint(self, checkpoint_name, run_id=None):
        if not checkpoint_name:
            raise ValueError("You must provide a 'checkpoint_name'.")

        if run_id is None:
            run_id = RunIdentifier(run_name=f"run_{checkpoint_name}")

        elif run_id:
            run_id = RunIdentifier(run_name=run_id)

        try:
            checkpoint = self.context.checkpoints.get(checkpoint_name)
            results = checkpoint.run(run_id=run_id, batch_parameters=self.batch_parameters)
            print(f"Result of the run: {results.success}")
            return results
        except Exception as e:
            raise ValueError(f"Error running checkpoint '{checkpoint_name}': {e}")

    


    

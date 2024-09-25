classdef RadioMLDatastore < matlab.io.Datastore
    % H5Datastore - datastore for a single HDF5 file
    % This object provides a datastore interface for a single HDF5 file.

    properties (SetAccess=protected)
        DatasetName char % Name of the dataset to read from
        BatchSize uint32 % Number of rows to read in each batch
        CurrentRow uint32 % Current row position in the dataset
        LabelName char
        SamplesPerSNR uint32
        DerivedStride uint32
    end
    
    properties (Access=private)
        FilePath char % Path to the HDF5 file
        DatasetInfo struct % Information about the dataset
        LabelInfo struct
        DSMode char
        DSModeConfig struct
        Start double
        LabelTags
    end

    methods
        function obj = RadioMLDatastore(filePath, dataName, labelName, mode, batchSize, stride, start)
            % Constructor
            obj.FilePath = filePath;
            obj.DatasetName = dataName;
            obj.LabelName = labelName;
            obj.DatasetInfo = h5info(filePath, dataName);
            obj.LabelInfo = h5info(filePath, labelName);
            obj.Start = start;
            
            % -----Unique to RadioML Dataset-----% 
            
            % number of frames per SNR
            obj.SamplesPerSNR = 4096; 
            obj.LabelTags = fileread('classes-fixed.json');
            obj.LabelTags = jsondecode(obj.LabelTags);

            % sampling a single frame per SNR for each modulation scheme
            % results in a batchsize of #642
            if nargin < 5
                obj.BatchSize = 624;
            else
                obj.BatchSize = batchSize;
            end

            % appropriate stride to properly sample RadioML Dataset
            % for each mini batch
            if nargin < 6
                obj.DerivedStride = 4096;
            else
                obj.DerivedStride = stride;
            end

            % custom query logic for train, test & validation data fetch
            obj.DSMode = mode;
            obj.DSModeConfig = struct( ...
                'train', struct( ...
                    'startIdx', 411, ...
                    'endIdx', 3686, ...
                    'midIdx', -1, ...
                    'minStartIdx', -1 ...
                ), ...
                'test', struct( ...
                    'startIdx', 1, ...
                    'midIdx', 205, ...
                    'minStartIdx', 3687, ...
                    'endIdx', 3891 ...
                ), ...
                'validate', struct( ...
                    'startIdx', 206, ...
                    'midIdx', 410, ...
                    'minStartIdx', 3892, ...
                    'endIdx', 4096 ...
                ) ...
            );

            % initialize start row
            if nargin < 7
                obj.CurrentRow = obj.DSModeConfig.(obj.DSMode).startIdx;
                obj.Start = obj.CurrentRow;
            else
                obj.CurrentRow = start;
            end
        end
        
        function tf = hasdata(obj)
            % Returns true if more data is available.
            if obj.DSMode == "train"
                tf = obj.CurrentRow <= obj.DSModeConfig.train.endIdx;
            elseif obj.DSMode == "test"
                tf = obj.CurrentRow <= obj.DSModeConfig.test.endIdx;
            elseif obj.DSMode == "validate"
                tf = obj.CurrentRow <= obj.DSModeConfig.validate.endIdx;
            end
        end
        
        function [data, info] = read(obj)
            % Read data in batches from the HDF5 file.
            numRows = obj.DatasetInfo.Dataspace.Size(3);
            startRow = obj.CurrentRow;
            endRow = min(startRow + obj.BatchSize - 1, numRows);
            
            % Calculate size to read
            dataSize = [obj.DatasetInfo.Dataspace.Size(1:2) obj.BatchSize];
            startIdx = [ones(1, numel(dataSize) - 1), startRow];
            decimation = [ones(1, numel(dataSize) - 1), obj.DerivedStride]; % Adjust if decimation is required

            labelSize = [obj.LabelInfo.Dataspace.Size(1) obj.BatchSize];
            labelStartIdx = [ones(1, numel(labelSize) - 1), startRow];
            labelDecimation = [ones(1, numel(labelSize) - 1), obj.DerivedStride]; % Adjust if decimation is required
  
            
            % Convert to double precision
            startIdx = double(startIdx);
            dataSize = double(dataSize);
            decimation = double(decimation);
            

            labelSize = double(labelSize);
            labelStartIdx = double(labelStartIdx);
            labelDecimation = double(labelDecimation);
            
            % Read the data
            data = h5read(obj.FilePath, obj.DatasetName, startIdx, dataSize, decimation);

            label = h5read(obj.FilePath, obj.LabelName, labelStartIdx, labelSize, labelDecimation);
            [~, classIndices] = max(label, [], 1);
            classLabels = arrayfun(@(x) obj.LabelTags{x}, classIndices, 'UniformOutput', false);
            classLabels = string(classLabels);
            label = categorical(classLabels, string(obj.LabelTags));
            
            % Update current row
            if obj.DSMode == "validate"
                if obj.CurrentRow == obj.DSModeConfig.validate.midIdx
                    obj.CurrentRow = obj.DSModeConfig.validate.minStartIdx;
                else
                    obj.CurrentRow = obj.CurrentRow + 1;
                end
            elseif obj.DSMode == "test"
                if obj.CurrentRow == obj.DSModeConfig.test.midIdx
                    obj.CurrentRow = obj.DSModeConfig.test.minStartIdx;
                else
                    obj.CurrentRow = obj.CurrentRow + 1;
                end
                
            else
                obj.CurrentRow = obj.CurrentRow + 1;
            end
            
            
            % Output info
            info = struct('StartRow', startRow, 'EndRow', endRow, 'Labels', label);
        end
        
        function reset(obj)
            % Reset to the beginning of the dataset
            obj.CurrentRow = obj.Start;
        end
        
        function frac = progress(obj)
            % Return progress as a fraction
            % numRows = obj.DatasetInfo.Dataspace.Size(3);
            frac = (obj.CurrentRow) / obj.SamplesPerSNR;
        end
    end
end
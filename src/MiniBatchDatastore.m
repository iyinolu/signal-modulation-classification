classdef MiniBatchDatastore < matlab.io.Datastore & matlab.io.datastore.MiniBatchable
    properties
        H5DS
        MiniBatchSize
      
    end

    properties(SetAccess = protected)
        NumObservations
    end
    
    methods
        function obj = MiniBatchDatastore(h5ds, miniBatchSize)
            obj.H5DS = h5ds;
            obj.MiniBatchSize = miniBatchSize;
        end
        
        function [data, info] = read(obj)
            [result, info] = obj.H5DS.read();
            result = permute(result,[3,2,1]);
            result = mat2cell(result, ones(obj.MiniBatchSize, 1), 1024, 2);
            data = cell2table(result, 'VariableNames', {'Predictors'});
            data.Response = info.Labels(:);

            info = [];
        end
        
        function tf = hasdata(obj)
            tf = obj.H5DS.hasdata();
        end
        
        function reset(obj)
            obj.H5DS.reset();
        end
        
        function frac = progress(obj)
            frac = obj.H5DS.progress();
        end

        function [data, info] = next(obj)
            [data, info] = obj.read();
        end
    end
end